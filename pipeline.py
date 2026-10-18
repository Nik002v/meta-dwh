"""Small local Meta DWH: CSV/JSONL -> Python normalization -> DuckDB SQL.

The source registry is trusted and stays outside the warehouse. There is no
Unknown-team fallback. One transaction includes dimensions, facts, bridges,
issues, load audit and both source checkpoints.
"""
import argparse
import csv
import hashlib
import json
import math
import re
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent
UTC = timezone.utc
METRICS = {
    'cpuTime': ('cpu_time_ms', 'duration'),
    'wallTime': ('wall_time_ms', 'duration'),
    'queuedTime': ('queued_time_ms', 'duration'),
    'physicalInputBytes': ('physical_input_bytes', 'integer'),
    'peakUserMemoryBytes': ('peak_user_memory_bytes', 'integer'),
    'writtenBytes': ('written_bytes', 'integer'),
    'writtenRows': ('written_rows', 'integer'),
    'spilledBytes': ('spilled_bytes', 'integer'),
}
UNITS_MS = {'ns': 0.000001, 'us': 0.001, 'ms': 1, 's': 1000,
            'm': 60000, 'h': 3600000, 'd': 86400000}


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError('Expected ISO-8601 timestamp with timezone')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Timestamp has no timezone')
    return result.astimezone(UTC)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def digest(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def insert(con, table, row, returning=None):
    # Identifiers only come from this module, never from source data or CLI text.
    columns = ', '.join(row)
    marks = ', '.join('?' for _ in row)
    sql = f'INSERT INTO {table} ({columns}) VALUES ({marks})'
    if returning:
        sql += f' RETURNING {returning}'
    result = con.execute(sql, list(row.values()))
    return result.fetchone()[0] if returning else None


class Registry:
    """Read a trusted external export; resolve business validity in [from, to)."""

    def __init__(self, package):
        for attr, name in [('users', 'users_history.csv'), ('teams', 'teams_history.csv'),
                           ('memberships', 'user_team_membership.csv')]:
            with (package / name).open(encoding='utf-8', newline='') as handle:
                rows = list(csv.DictReader(handle))
            for row in rows:
                for field in ('valid_from', 'valid_to', 'updated_at'):
                    row[field] = timestamp(row[field]) if row[field] else None
                if 'is_active' in row:
                    row['is_active'] = row['is_active'].lower() == 'true'
            setattr(self, attr, rows)

    @staticmethod
    def valid(row, at):
        return row['valid_from'] <= at and (row['valid_to'] is None or at < row['valid_to'])

    @staticmethod
    def one(rows):
        if len(rows) != 1:
            # A broken source contract aborts the batch; it does not invent a team.
            raise LookupError('Registry contract violated: expected exactly one matching row')
        return rows[0]

    def resolve(self, username, at):
        user = self.one([row for row in self.users
                         if row['trino_username'] == username and self.valid(row, at)])
        member = self.one([row for row in self.memberships
                           if row['user_id'] == user['user_id'] and self.valid(row, at)])
        team = self.one([row for row in self.teams
                         if row['team_id'] == member['team_id'] and self.valid(row, at)])
        return user['user_id'], team['team_id']

    def current(self, kind, cutoff):
        at = cutoff - timedelta(microseconds=1)
        return [row for row in getattr(self, kind) if self.valid(row, at)]


def update_profiles(con, registry, cutoff, previous, now, batch, counts):
    for kind, table, id_field, names in [
        ('teams', 'dim_team', 'team_id', ['team_name', 'is_active']),
        ('users', 'dim_user', 'user_id', ['trino_username', 'display_name', 'account_type', 'is_active']),
    ]:
        for profile in registry.current(kind, cutoff):
            if previous and profile['updated_at'] < previous:
                continue
            values = {('username' if field == 'trino_username' else field): profile[field] for field in names}
            attribute_names = list(values)
            existing = con.execute(
                f'SELECT source_updated_at, {", ".join(attribute_names)} FROM {table} WHERE {id_field} = ?',
                [profile[id_field]]).fetchone()
            values['source_updated_at'] = profile['updated_at']
            if existing is None:
                insert(con, table, {id_field: profile[id_field], **values,
                                   'dwh_inserted_at': now, 'dwh_updated_at': now, 'load_batch_id': batch})
                counts['profiles_inserted'] += 1
            elif profile['updated_at'] > existing[0]:
                # In particular, do not UPDATE an unchanged indexed username.
                previous_values = dict(zip(attribute_names, existing[1:]))
                values = {key: value for key, value in values.items()
                          if key == 'source_updated_at' or previous_values[key] != value}
                values.update(dwh_updated_at=now, load_batch_id=batch)
                assignments = ', '.join(f'{field} = ?' for field in values)
                con.execute(f'UPDATE {table} SET {assignments} WHERE {id_field} = ?',
                            [*values.values(), profile[id_field]])
                counts['profiles_updated'] += 1


def reassign_teams(con, registry, previous, cutoff, now, batch, counts):
    if previous is None or not any(previous <= row['updated_at'] < cutoff for row in registry.memberships):
        return
    # Small-demo implementation: inspect existing facts when membership changed.
    # A larger deployment would restrict this to affected accounts/time ranges.
    facts = con.execute('''SELECT f.query_key, u.username, f.created_at, f.team_key
                           FROM fact_query_execution f JOIN dim_user u USING (user_key)''').fetchall()
    for query_key, username, created_at, old_team in facts:
        _, team_id = registry.resolve(username, created_at)
        team_key = con.execute('SELECT team_key FROM dim_team WHERE team_id = ?', [team_id]).fetchone()[0]
        if team_key != old_team:
            con.execute('''UPDATE fact_query_execution SET team_key = ?, dwh_updated_at = ?, load_batch_id = ?
                           WHERE query_key = ?''', [team_key, now, batch, query_key])
            counts['facts_reassigned'] += 1


def issue(con, record, kind, reason, batch, now, counts):
    event = record.get('event') or {}
    metadata = event.get('metadata') if isinstance(event, dict) else None
    metadata = metadata if isinstance(metadata, dict) else {}
    payload = canonical(record)
    issue_id = digest(canonical([kind, reason, record.get('raw_record_id'), payload]))
    exists = con.execute('SELECT 1 FROM etl_issue WHERE issue_id = ?', [issue_id]).fetchone()
    if not exists:
        insert(con, 'etl_issue', dict(issue_id=issue_id, batch_id=batch,
               raw_record_id=record.get('raw_record_id'), cluster_id=record.get('cluster_id'),
               query_id=metadata.get('queryId'), issue_type=kind, reason=reason,
               raw_payload=payload, dwh_inserted_at=now))
        counts['issues_logged'] += 1


def event_hash(event):
    """Relevant source content; excludes ingestion envelope and unused diagnostics."""
    projection = {field: event.get(field) for field in ('createTime', 'executionStartTime', 'endTime')}
    projection['metadata'] = {key: event['metadata'].get(key)
                              for key in ('queryId', 'query', 'queryState', 'transactionId')}
    projection['context'] = {key: event.get('context', {}).get(key) for key in (
        'user', 'queryType', 'source', 'resourceGroupId', 'retryPolicy', 'serverVersion', 'catalog', 'schema')}
    projection['statistics'] = {key: event.get('statistics', {}).get(key) for key in (*METRICS, 'complete')}
    io = event.get('ioMetadata') or {}
    projection['inputs'] = sorted([
        {key: item.get(key) for key in ('catalogName', 'schema', 'table', 'physicalInputBytes', 'physicalInputRows')}
        for item in io.get('inputs', [])], key=canonical)
    output = io.get('output')
    projection['output'] = None if output is None else {
        key: output.get(key) for key in ('catalogName', 'schema', 'table')}
    failure = event.get('failureInfo') or {}
    projection['error_code'] = (failure.get('errorCode') or {}).get('name')
    return digest(canonical(projection))


def metric(value, kind):
    if value is None:
        return None
    if kind == 'integer':
        if type(value) is not int or not 0 <= value <= 9223372036854775807:
            raise ValueError('expected a nonnegative signed-64-bit integer')
        return value
    match = re.fullmatch(r'([+-]?(?:\d+(?:\.\d*)?|\.\d+))(ns|us|ms|s|m|h|d)', str(value))
    if not match:
        raise ValueError('expected a duration with unit ns/us/ms/s/m/h/d')
    result = float(match[1]) * UNITS_MS[match[2]]
    if not math.isfinite(result) or result < 0:
        raise ValueError('expected a finite nonnegative duration')
    return result


def table_identity(item):
    identity = tuple(item.get(field) for field in ('catalogName', 'schema', 'table'))
    if not all(isinstance(value, str) and value for value in identity):
        raise ValueError('Incomplete table identity in ioMetadata')
    return identity


def prepare(record):
    event = record['event']
    metadata, context = event['metadata'], event['context']
    created, ended = timestamp(event['createTime']), timestamp(event['endTime'])
    if ended < created:
        raise ValueError('endTime precedes createTime')
    if metadata['queryState'] not in ('FINISHED', 'FAILED'):
        raise ValueError('Completed event has a nonterminal queryState')
    if not isinstance(metadata.get('query'), str) or not context.get('user'):
        raise ValueError('Missing SQL text or effective user')
    started = event.get('executionStartTime')
    started = timestamp(started) if started else None
    # Do not assume all early failures have a meaningful worker-start timestamp.
    if started and (started < created or started > ended):
        started = None
    for item in (event.get('ioMetadata') or {}).get('inputs', []):
        table_identity(item)
    output = (event.get('ioMetadata') or {}).get('output')
    if output is not None:
        table_identity(output)
    return created, started, ended


def ensure_table(con, identity, now, batch):
    row = con.execute('''SELECT table_key FROM dim_table
                         WHERE catalog_name = ? AND schema_name = ? AND table_name = ?''', identity).fetchone()
    return row[0] if row else insert(con, 'dim_table', dict(
        zip(('catalog_name', 'schema_name', 'table_name', 'dwh_inserted_at', 'load_batch_id'),
            (*identity, now, batch))), 'table_key')


def insert_fact(con, registry, record, fingerprint, now, batch, counts):
    event = record['event']
    created, started, ended = prepare(record)  # Reject before any dimensional side effects.
    metadata, context, stats = event['metadata'], event['context'], event.get('statistics', {})
    user_id, team_id = registry.resolve(context['user'], created)
    user_key = con.execute('SELECT user_key FROM dim_user WHERE user_id = ?', [user_id]).fetchone()[0]
    team_key = con.execute('SELECT team_key FROM dim_team WHERE team_id = ?', [team_id]).fetchone()[0]
    sql = metadata['query']
    sql_hash = digest(sql)
    text = con.execute('SELECT query_text_key, sql_text FROM dim_query_text WHERE sql_hash = ?', [sql_hash]).fetchone()
    if text and text[1] != sql:
        raise RuntimeError('SQL hash collision')
    text_key = text[0] if text else insert(con, 'dim_query_text', dict(
        sql_hash=sql_hash, sql_text=sql, dwh_inserted_at=now, load_batch_id=batch), 'query_text_key')
    day = ended.date()
    date_key = int(day.strftime('%Y%m%d'))
    con.execute('''INSERT INTO dim_date VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING''',
                [date_key, day, day.year, day.month, day - timedelta(days=day.weekday()), day.isoweekday()])
    measures, invalid = {}, False
    for source_field, (target_field, kind) in METRICS.items():
        try:
            measures[target_field] = metric(stats.get(source_field), kind)
        except ValueError as exc:
            measures[target_field], invalid = None, True
            issue(con, record, 'INVALID_METRIC', f'{source_field}: {exc}', batch, now, counts)
    failure = event.get('failureInfo') or {}
    row = dict(cluster_id=record['cluster_id'], query_id=metadata['queryId'], user_key=user_key,
               team_key=team_key, completed_date_key=date_key, query_text_key=text_key,
               created_at=created, execution_started_at=started, completed_at=ended, **measures,
               query_state=metadata['queryState'], query_type=context.get('queryType'), source=context.get('source'),
               resource_group_id='.'.join(context['resourceGroupId']) if context.get('resourceGroupId') else None,
               retry_policy=context.get('retryPolicy'), error_code=(failure.get('errorCode') or {}).get('name'),
               statistics_complete=stats.get('complete'), trino_version=context.get('serverVersion'),
               transaction_id=metadata.get('transactionId'), has_invalid_metrics=invalid,
               source_event_hash=fingerprint, source_raw_record_id=record['raw_record_id'],
               source_ingested_at=timestamp(record['raw_ingested_at']),
               dwh_inserted_at=now, dwh_updated_at=now, load_batch_id=batch)
    query_key = insert(con, 'fact_query_execution', row, 'query_key')
    inputs = defaultdict(list)
    for item in (event.get('ioMetadata') or {}).get('inputs', []):
        observations = []
        for field in ('physicalInputBytes', 'physicalInputRows'):
            try:
                observations.append(metric(item.get(field), 'integer'))
            except ValueError as exc:
                observations.append(None)
                invalid = True
                issue(con, record, 'INVALID_METRIC', f'input {table_identity(item)} {field}: {exc}', batch, now, counts)
        inputs[table_identity(item)].append(observations)
    for identity, observations in inputs.items():
        positive = any(value is not None and value > 0 for pair in observations for value in pair)
        # A zero counter for each occurrence suffices; contradictory positives win.
        zero = all(any(value == 0 for value in pair) for pair in observations)
        evidence = 'POSITIVE' if positive else ('ZERO' if zero else 'UNKNOWN')
        insert(con, 'bridge_query_table', dict(query_key=query_key, table_key=ensure_table(con, identity, now, batch),
               access_role='INPUT', read_evidence=evidence, dwh_inserted_at=now, load_batch_id=batch))
    output = (event.get('ioMetadata') or {}).get('output')
    if output is not None:
        insert(con, 'bridge_query_table', dict(query_key=query_key,
               table_key=ensure_table(con, table_identity(output), now, batch), access_role='WRITE_TARGET',
               read_evidence='NOT_APPLICABLE', dwh_inserted_at=now, load_batch_id=batch))
    if invalid and not row['has_invalid_metrics']:
        con.execute('UPDATE fact_query_execution SET has_invalid_metrics = true WHERE query_key = ?', [query_key])
    counts['facts_inserted'] += 1


def process_events(con, registry, package, previous, cutoff, now, batch, counts):
    groups = defaultdict(list)
    with (package / 'events.jsonl').open(encoding='utf-8') as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                # Counters must be integers; durations must include units.
                # Keep decimal/exponent tokens as text so overflow cannot create inf.
                record = json.loads(line, parse_float=str,
                                    parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
            except ValueError:
                record = {'unreadable_line': line.rstrip('\n')}
                issue(con, record, 'REJECTED', 'Unreadable JSON record', batch, now, counts)
                counts['records_rejected'] += 1
                continue
            # Envelope times are a source contract. They drive incremental selection.
            ingested = timestamp(record['raw_ingested_at'])
            if (previous and ingested < previous) or ingested >= cutoff:
                continue
            counts['raw_selected'] += 1
            if record.get('event_type') == 'QueryCreated':
                counts['created_ignored'] += 1
                continue
            try:
                if record.get('event_type') != 'QueryCompleted':
                    raise ValueError('Unsupported event type')
                event = record['event']
                qid = event['metadata'].get('queryId')
                if not isinstance(qid, str) or not qid or not record.get('cluster_id'):
                    raise ValueError('Missing query identity')
                fingerprint = event_hash(event)
            except (ValueError, KeyError, TypeError, AttributeError) as exc:
                issue(con, record, 'REJECTED', str(exc), batch, now, counts)
                counts['records_rejected'] += 1
                continue
            groups[(record['cluster_id'], qid)].append((record, fingerprint))
    for identity, copies in sorted(groups.items()):
        copies.sort(key=lambda pair: (timestamp(pair[0]['raw_ingested_at']), pair[0]['raw_record_id']))
        existing = con.execute('''SELECT query_key, source_event_hash, has_source_conflict
                                  FROM fact_query_execution WHERE cluster_id = ? AND query_id = ?''', identity).fetchone()
        if existing:
            for record, fingerprint in copies:
                if fingerprint == existing[1]:
                    counts['duplicates_skipped'] += 1
                else:
                    issue(con, record, 'CONFLICT', 'Content differs from accepted completed event', batch, now, counts)
                    if not existing[2]:
                        con.execute('''UPDATE fact_query_execution SET has_source_conflict = true,
                                       dwh_updated_at = ?, load_batch_id = ? WHERE query_key = ?''',
                                    [now, batch, existing[0]])
                    counts['conflicting_records'] += 1
        elif len({fingerprint for _, fingerprint in copies}) > 1 or con.execute(
                "SELECT 1 FROM etl_issue WHERE cluster_id = ? AND query_id = ? AND issue_type = 'CONFLICT' LIMIT 1",
                identity).fetchone():
            for record, _ in copies:
                issue(con, record, 'CONFLICT', 'Conflicting completed events; identity requires explicit resolution', batch, now, counts)
                counts['conflicting_records'] += 1
        else:
            record, fingerprint = copies[0]
            # Validate before insert. Database/registry errors abort the transaction.
            try:
                prepare(record)
            except (ValueError, KeyError, TypeError, AttributeError) as exc:
                for rejected, _ in copies:
                    issue(con, rejected, 'REJECTED', str(exc), batch, now, counts)
                    counts['records_rejected'] += 1
                continue
            insert_fact(con, registry, record, fingerprint, now, batch, counts)
            counts['duplicates_skipped'] += len(copies) - 1


def load(mode, package, database):
    package, database = Path(package).resolve(), Path(database).resolve()
    if mode == 'init' and database.exists():
        raise FileExistsError('init requires a new database path; existing databases are never overwritten')
    if mode == 'incremental' and not database.exists():
        raise FileNotFoundError('Run init first')
    manifest = json.loads((package / 'manifest.json').read_text(encoding='utf-8'))
    cutoff = timestamp(manifest['available_until'])
    registry = Registry(package)
    database.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(database))
    now, batch, counts = datetime.now(UTC), str(uuid.uuid4()), Counter()
    try:
        con.execute("SET TimeZone = 'UTC'")
        con.execute('BEGIN TRANSACTION')
        if mode == 'init':
            con.execute((ROOT / 'sql' / 'schema.sql').read_text(encoding='utf-8'))
        checkpoints = dict(con.execute('SELECT source_name, processed_until FROM etl_checkpoint').fetchall())
        if any(value > cutoff for value in checkpoints.values()):
            raise ValueError('Refusing a source snapshot older than the committed checkpoint')
        if all(checkpoints.get(source) == cutoff for source in ('query_events', 'registry')):
            con.execute('ROLLBACK')
            return {'package_id': manifest['package_id'], 'status': 'already_processed'}
        update_profiles(con, registry, cutoff, checkpoints.get('registry'), now, batch, counts)
        reassign_teams(con, registry, checkpoints.get('registry'), cutoff, now, batch, counts)
        process_events(con, registry, package, checkpoints.get('query_events'), cutoff, now, batch, counts)
        for source in ('query_events', 'registry'):
            con.execute('''INSERT INTO etl_checkpoint VALUES (?, ?)
                           ON CONFLICT (source_name) DO UPDATE SET processed_until = excluded.processed_until''',
                        [source, cutoff])
        summary = dict(counts)
        insert(con, 'etl_load', dict(batch_id=batch, package_id=manifest['package_id'], started_at=now,
               finished_at=datetime.now(UTC), processed_until=cutoff, summary_json=canonical(summary)))
        con.execute('COMMIT')
        return {'package_id': manifest['package_id'], 'status': 'loaded', **summary}
    except Exception:
        con.execute('ROLLBACK')
        raise
    finally:
        con.close()


def report(database, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database), read_only=True) as con:
        con.execute("SET TimeZone = 'UTC'")
        for path in sorted((ROOT / 'sql' / 'reports').glob('*.sql')):
            result = con.execute(path.read_text(encoding='utf-8'))
            headers, rows = [item[0] for item in result.description], result.fetchall()
            with (output / f'{path.stem}.csv').open('w', encoding='utf-8', newline='') as handle:
                writer = csv.writer(handle)
                writer.writerow(headers)
                writer.writerows(rows)
            print(f'\n{path.stem}\n' + ' | '.join(headers))
            for row in rows:
                print(' | '.join(str(value) for value in row))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('init', 'incremental', 'report'):
        command = commands.add_parser(name)
        command.add_argument('--db', type=Path, default=ROOT / 'warehouse' / 'meta_dwh.duckdb')
        if name != 'report':
            command.add_argument('--package', type=Path, default=ROOT / 'data' / name)
        else:
            command.add_argument('--output', type=Path, default=ROOT / 'outputs')
    args = parser.parse_args()
    if args.command == 'report':
        report(args.db, args.output)
    else:
        print(json.dumps(load(args.command, args.package, args.db), indent=2))


if __name__ == '__main__':
    main()
