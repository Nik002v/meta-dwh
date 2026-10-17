"""Generate two deterministic, cumulative snapshots. No database is required."""
import copy
import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
UTC = timezone.utc
EVENTS = ('iceberg', 'product_analytics', 'feature_events')
USERS = ('iceberg', 'product_analytics', 'users')
DAILY = ('iceberg', 'product_analytics', 'daily_usage')
INVOICES = ('iceberg', 'billing', 'invoices')
REPEATED_SQL = ('SELECT count(*) FROM iceberg.product_analytics.feature_events e '
                'JOIN iceberg.product_analytics.users u ON e.user_id = u.id')


def stamp(value):
    return value.isoformat().replace('+00:00', 'Z')


def table_ref(identity, size=None, rows=None):
    catalog, schema, table = identity
    return dict(catalogName=catalog, schema=schema, table=table,
                physicalInputBytes=size, physicalInputRows=rows)


def completed(qid, user, day, cpu, size, inputs, sql=None, state='FINISHED',
              output=None, wall=1000, written=0, complete=True):
    start = datetime.fromisoformat(day.replace('Z', '+00:00'))
    return {
        'createTime': stamp(start),
        'executionStartTime': stamp(start + timedelta(milliseconds=100)),
        'endTime': stamp(start + timedelta(milliseconds=wall)),
        'metadata': {'queryId': qid, 'query': sql or f'SELECT count(*) FROM {".".join(EVENTS)}',
                     'queryState': state, 'transactionId': f'tx-{qid}'},
        'context': {'user': user, 'queryType': 'SELECT', 'source': 'demo-client',
                    'resourceGroupId': ['global', 'adhoc'], 'retryPolicy': 'NONE',
                    'serverVersion': '484', 'catalog': 'iceberg', 'schema': 'product_analytics'},
        'statistics': {'cpuTime': None if cpu is None else f'{cpu}ms',
                       'wallTime': f'{wall}ms', 'queuedTime': '100ms',
                       'physicalInputBytes': size, 'peakUserMemoryBytes': 1048576,
                       'writtenBytes': written, 'writtenRows': written // 100,
                       'spilledBytes': 0, 'complete': complete},
        'ioMetadata': {'inputs': inputs, 'output': None if output is None else {
            'catalogName': output[0], 'schema': output[1], 'table': output[2]}},
        'failureInfo': None if state == 'FINISHED' else {
            'errorCode': {'name': 'GENERIC_INTERNAL_ERROR'}, 'failureMessage': 'Synthetic failure'},
    }


def envelope(rid, event, ingested=None, event_type='QueryCompleted'):
    return {'raw_record_id': rid, 'raw_ingested_at': ingested or event['endTime'],
            'cluster_id': 'demo-cluster', 'event_type': event_type, 'event': copy.deepcopy(event)}


def write_csv(path, rows):
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def generate():
    jan = '2026-01-01T00:00:00Z'
    feb = '2026-02-01T00:00:00Z'
    changed = '2026-02-02T00:00:00Z'
    users = [dict(user_id=uid, trino_username=name, display_name=display,
                  account_type=kind, is_active='true', valid_from=jan, valid_to='', updated_at=jan)
             for uid, name, display, kind in [
                 ('U_ANA', 'ana', 'Ana Petrovic', 'HUMAN'),
                 ('U_BORIS', 'boris', 'Boris Ilic', 'HUMAN'),
                 ('U_ETL', 'svc_etl', 'ETL service', 'SERVICE'),
                 ('U_REPORT', 'svc_report', 'Reporting service', 'SERVICE')]]
    teams = [dict(team_id=tid, team_name=name, is_active='true',
                  valid_from=jan, valid_to='', updated_at=jan)
             for tid, name in [('T_ANALYTICS', 'Analytics'), ('T_PLATFORM', 'Platform')]]
    memberships = [dict(user_id=user['user_id'], team_id=team,
                        valid_from=jan, valid_to='', updated_at=jan)
                   for user, team in zip(users, ['T_ANALYTICS', 'T_PLATFORM', 'T_PLATFORM', 'T_ANALYTICS'])]

    q1 = completed('q001', 'ana', '2026-01-10T10:00:00Z', 60000, 3000,
                   [table_ref(EVENTS, 1000, 10), table_ref(USERS, 2000, 20)], REPEATED_SQL, wall=10000)
    initial = [
        envelope('r-created-001', {key: q1[key] for key in ('createTime', 'metadata', 'context')},
                 q1['createTime'], 'QueryCreated'),
        envelope('r001', q1),
        envelope('r002', completed('q002', 'boris', '2026-01-11T10:00:00Z', 20000, 1200,
                 [table_ref(EVENTS, 1000, 10), table_ref(USERS, 200, 2)], REPEATED_SQL)),
        envelope('r003', completed('q003', 'ana', '2026-01-18T10:00:00Z', 5000, 100,
                 [table_ref(EVENTS, 100, 1)], state='FAILED')),
        envelope('r004', completed('q004', 'svc_etl', '2026-01-20T10:00:00Z', 15000, 2000,
                 [table_ref(EVENTS, 2000, 20)],
                 'INSERT INTO iceberg.product_analytics.daily_usage SELECT * FROM iceberg.product_analytics.feature_events',
                 output=DAILY, written=9000)),
        envelope('r005', completed('q005', 'ana', '2026-01-21T10:00:00Z', 1, 0, [], 'SELECT 1')),
        envelope('r006', completed('q006', 'boris', '2026-01-22T10:00:00Z', 8000, 500,
                 [table_ref(EVENTS, 300, 3), table_ref(EVENTS, 200, 2)],
                 'SELECT count(*) FROM iceberg.product_analytics.feature_events a JOIN iceberg.product_analytics.feature_events b ON a.user_id=b.user_id')),
        envelope('r007', completed('q007', 'svc_report', '2026-01-23T10:00:00Z', None, None,
                 [table_ref(EVENTS)], complete=False)),
        envelope('r001-copy', q1, '2026-01-30T10:00:00Z'),
    ]
    initial[0]['event']['metadata']['queryState'] = 'QUEUED'
    initial[4]['event']['context']['queryType'] = 'INSERT'

    users2, teams2, memberships2 = copy.deepcopy((users, teams, memberships))
    users2[0].update(valid_to=changed, updated_at=changed)
    users2.append(dict(users[0], display_name='Ana Jovanovic', valid_from=changed, updated_at=changed))
    users2.append(dict(users[0], user_id='U_MILA', trino_username='mila', display_name='Mila Jovic',
                       valid_from=feb, updated_at=feb))
    teams2[0].update(valid_to=changed, updated_at=changed)
    teams2.append(dict(teams[0], team_name='Product Analytics', valid_from=changed, updated_at=changed))
    teams2.append(dict(teams[0], team_id='T_RESEARCH', team_name='Research', valid_from=feb, updated_at=feb))
    memberships2[0].update(valid_to=feb, updated_at=feb)
    memberships2.extend([
        dict(memberships[0], team_id='T_PLATFORM', valid_from=feb, updated_at=feb),
        dict(memberships[0], user_id='U_MILA', team_id='T_RESEARCH', valid_from=feb, updated_at=feb)])

    additions = [
        envelope('r101', completed('q101', 'ana', '2026-02-05T10:00:00Z', 30000, 1500,
                 [table_ref(EVENTS, 1000, 10), table_ref(USERS, 500, 5)], REPEATED_SQL)),
        # Exactly on the previous exclusive cutoff: the incremental load must include it.
        envelope('r102', completed('q102', 'boris', '2026-01-31T23:59:00Z', 7000, 0,
                 [table_ref(EVENTS, 0, 0)], "SELECT * FROM iceberg.product_analytics.feature_events WHERE event_date = DATE '1900-01-01'"), feb),
        envelope('r103', completed('q103', 'ana', '2026-02-07T10:00:00Z', 4000, 500,
                 [table_ref(EVENTS, 500, 5)],
                 'INSERT INTO iceberg.product_analytics.daily_usage SELECT * FROM iceberg.product_analytics.feature_events',
                 state='FAILED', output=DAILY, written=2000)),
        envelope('r104', completed('q104', 'svc_etl', '2026-02-08T10:00:00Z', 6000, 200,
                 [table_ref(DAILY, 200, 2)],
                 'UPDATE iceberg.product_analytics.daily_usage SET count = count + 1', output=DAILY, written=4000)),
        envelope('r105', completed('q105', 'mila', '2026-02-09T10:00:00Z', 9000, 900,
                 [table_ref(INVOICES, 900, 9)], 'SELECT * FROM iceberg.billing.invoices')),
        envelope('r106', completed('q106', 'svc_report', '2026-02-10T10:00:00Z', -50, -1,
                 [table_ref(EVENTS)], 'SELECT sum(value) FROM iceberg.product_analytics.feature_events')),
        envelope('r008-late', completed('q008', 'ana', '2026-01-15T10:00:00Z', 11000, 1100,
                 [table_ref(EVENTS, 1100, 11)]), '2026-02-03T10:00:00Z'),
    ]
    additions[2]['event']['context']['queryType'] = 'INSERT'
    additions[3]['event']['context']['queryType'] = 'UPDATE'
    conflict = completed('q107', 'boris', '2026-02-11T10:00:00Z', 100, 10, [table_ref(EVENTS, 10, 1)])
    additions.extend([envelope('r107-a', conflict), envelope('r107-b', conflict, '2026-02-11T11:00:00Z')])
    additions[-1]['event']['statistics']['cpuTime'] = '200ms'
    additions.append(envelope('r001-conflict', q1, '2026-02-12T10:00:00Z'))
    additions[-1]['event']['statistics']['cpuTime'] = '999000ms'
    additions.append(envelope('r002-copy', initial[2]['event'], '2026-02-12T11:00:00Z'))
    bad_time = completed('q108', 'ana', '2026-02-13T10:00:00Z', 100, 1, [])
    bad_time['endTime'] = '2026-02-12T10:00:00Z'
    additions.append(envelope('r108-bad-time', bad_time, '2026-02-13T11:00:00Z'))
    no_id = completed('q109', 'ana', '2026-02-14T10:00:00Z', 100, 1, [])
    del no_id['metadata']['queryId']
    additions.append(envelope('r109-no-id', no_id))

    for name, cutoff, records, profiles, groups, links in [
        ('init', feb, initial, users, teams, memberships),
        ('incremental', '2026-03-01T00:00:00Z', initial + additions, users2, teams2, memberships2),
    ]:
        folder = ROOT / 'data' / name
        folder.mkdir(parents=True, exist_ok=True)
        (folder / 'manifest.json').write_text(json.dumps({
            'package_id': name, 'available_until': cutoff,
            'description': 'Cumulative source snapshot; cutoff is exclusive.'}, indent=2) + '\n', encoding='utf-8')
        (folder / 'events.jsonl').write_text(''.join(json.dumps(record, ensure_ascii=False) + '\n'
                                                   for record in records), encoding='utf-8')
        write_csv(folder / 'users_history.csv', profiles)
        write_csv(folder / 'teams_history.csv', groups)
        write_csv(folder / 'user_team_membership.csv', links)
        print(f'{name}: {len(records)} raw records -> {folder}')


if __name__ == '__main__':
    generate()
