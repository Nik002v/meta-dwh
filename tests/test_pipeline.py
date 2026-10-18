"""Business invariants and transactional/replay behavior; stdlib unittest only."""
import copy
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import pipeline


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='meta-dwh-')
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.db = self.folder / 'test.duckdb'
        self.initial = pipeline.load('init', ROOT / 'data' / 'init', self.db)

    def rows(self, sql, parameters=None):
        with duckdb.connect(str(self.db), read_only=True) as con:
            con.execute("SET TimeZone = 'UTC'")
            return con.execute(sql, parameters or []).fetchall()

    def incremental(self):
        return pipeline.load('incremental', ROOT / 'data' / 'incremental', self.db)

    def snapshot(self, include_technical=True):
        tables = [row[0] for row in self.rows('SHOW TABLES')]
        return {table: self.rows(f'SELECT * FROM {table} ORDER BY ALL') for table in tables
                if include_technical or not table.startswith('etl_')}

    def later_package(self):
        package = self.folder / 'later'
        shutil.copytree(ROOT / 'data' / 'incremental', package)
        (package / 'manifest.json').write_text(json.dumps({
            'package_id': 'later', 'available_until': '2026-03-02T00:00:00Z'}), encoding='utf-8')
        return package

    def append_record(self, package, record):
        with (package / 'events.jsonl').open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(record) + '\n')

    def test_initial_grain_and_multi_table_query(self):
        self.assertEqual(self.initial['facts_inserted'], 7)
        self.assertEqual(self.initial['duplicates_skipped'], 1)
        self.assertEqual(self.rows('SELECT COUNT(*), SUM(cpu_time_ms) FROM fact_query_execution'), [(7, 108001)])
        self.assertEqual(self.rows('SELECT COUNT(*) FROM bridge_query_table'), [(9,)])
        self.assertEqual(self.rows("SELECT cpu_time_ms > wall_time_ms FROM fact_query_execution WHERE query_id='q001'"), [(True,)])
        self.assertEqual(self.rows('''SELECT COUNT(*) FROM bridge_query_table b
            JOIN fact_query_execution f USING(query_key) WHERE f.query_id='q006' '''), [(1,)])

    def test_type1_history_late_event_and_cutoff_boundary(self):
        old_keys = self.rows('SELECT user_key, user_id FROM dim_user ORDER BY user_id')
        self.incremental()
        self.assertEqual(self.rows("SELECT user_key, user_id FROM dim_user WHERE user_id<>'U_MILA' ORDER BY user_id"), old_keys)
        self.assertEqual(self.rows("SELECT display_name FROM dim_user WHERE username='ana'"), [('Ana Jovanovic',)])
        self.assertEqual(self.rows("SELECT team_name FROM dim_team WHERE team_id='T_ANALYTICS'"), [('Product Analytics',)])
        self.assertEqual(self.rows('''SELECT f.query_id, t.team_id FROM fact_query_execution f
            JOIN dim_team t USING(team_key) WHERE f.query_id IN ('q001','q008','q101') ORDER BY f.query_id'''),
            [('q001', 'T_ANALYTICS'), ('q008', 'T_ANALYTICS'), ('q101', 'T_PLATFORM')])
        self.assertEqual(self.rows("SELECT completed_date_key FROM fact_query_execution WHERE query_id='q102'"), [(20260131,)])
        self.assertEqual(self.rows('SELECT COUNT(*) FROM dim_table'), [(4,)])
        self.assertEqual(self.rows('''SELECT COUNT(*) FROM bridge_query_table b
            JOIN fact_query_execution f USING(query_key) WHERE query_id='q104' '''), [(2,)])

    def test_reports_and_resource_totals(self):
        self.incremental()
        user_report = (ROOT / 'sql/reports/01_distinct_users.sql').read_text()
        self.assertEqual(self.rows(user_report), [(4,)])
        team = self.rows((ROOT / 'sql/reports/02_team_resources.sql').read_text())[0]
        self.assertEqual(team, ('T_PLATFORM', 'Platform', 7, 1, 90.0, 5900,
                                15000, 0, 1048576, 1000.0, 1000.0))
        quality = self.rows((ROOT / 'sql/reports/05_data_quality.sql').read_text())[0]
        self.assertEqual(quality, (14, 2, 175001.0, 11000, 2, 2, 1, 1, 2, 3, 2))
        self.assertEqual(self.rows('SELECT COUNT(*) FROM bridge_query_table'), [(19,)])
        self.assertEqual(self.rows('SELECT COUNT(*) FROM dim_query_text'), [(9,)])
        self.assertEqual(self.rows('''SELECT COUNT(*), SUM(f.cpu_time_ms) FROM fact_query_execution f
            JOIN dim_query_text q USING(query_text_key) WHERE q.sql_text LIKE 'SELECT count(*) FROM% e JOIN%' '''), [(3, 110000)])
        # svc_etl has only one execution with feature_events as INPUT (q004).
        # It must still count when its only matching execution is FAILED.
        with duckdb.connect(str(self.db)) as con:
            con.execute("UPDATE fact_query_execution SET query_state='FAILED' WHERE query_id='q004'")
        self.assertEqual(self.rows(user_report), [(4,)])
        # The same account must also count with ZERO as its only input evidence.
        with duckdb.connect(str(self.db)) as con:
            con.execute('''UPDATE bridge_query_table SET read_evidence='ZERO'
                WHERE query_key=(SELECT query_key FROM fact_query_execution WHERE query_id='q004')
                  AND access_role='INPUT' ''')
        self.assertEqual(self.rows(user_report), [(4,)])

    def test_missing_and_invalid_metrics_read_evidence(self):
        self.incremental()
        self.assertEqual(self.rows('''SELECT cpu_time_ms, physical_input_bytes, has_invalid_metrics
            FROM fact_query_execution WHERE query_id='q106' '''), [(None, None, True)])
        self.assertEqual(self.rows('''SELECT cpu_time_ms, statistics_complete, has_invalid_metrics
            FROM fact_query_execution WHERE query_id='q007' '''), [(None, False, False)])
        self.assertEqual(self.rows('''SELECT query_id, read_evidence FROM fact_query_execution
            JOIN bridge_query_table USING(query_key) WHERE query_id IN ('q007','q102') ORDER BY query_id'''),
            [('q007', 'UNKNOWN'), ('q102', 'ZERO')])

    def test_overflowing_numeric_metrics_do_not_abort_batch(self):
        self.incremental()
        package = self.later_package()
        record = json.loads((package / 'events.jsonl').read_text().splitlines()[1])
        record.update(raw_record_id='r-overflow', raw_ingested_at='2026-03-01T01:00:00Z')
        record['event']['metadata']['queryId'] = 'q-overflow'
        record['event']['statistics']['physicalInputBytes'] = 'OVERFLOW_TOKEN'
        record['event']['ioMetadata']['inputs'] = [{
            'catalogName': 'iceberg', 'schema': 'product_analytics',
            'table': 'feature_events', 'physicalInputBytes': 'OVERFLOW_TOKEN'}]
        with (package / 'events.jsonl').open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(record).replace('"OVERFLOW_TOKEN"', '1e309') + '\n')
        result = pipeline.load('incremental', package, self.db)
        self.assertEqual(result['facts_inserted'], 1)
        self.assertEqual(self.rows('''SELECT cpu_time_ms, physical_input_bytes, has_invalid_metrics,
            read_evidence FROM fact_query_execution JOIN bridge_query_table USING(query_key)
            WHERE query_id='q-overflow' '''), [(60000.0, None, True, 'UNKNOWN')])
        issues = self.rows("SELECT issue_type, raw_payload FROM etl_issue WHERE query_id='q-overflow'")
        self.assertEqual(len(issues), 2)
        self.assertTrue(all(kind == 'INVALID_METRIC' and '1e309' in payload for kind, payload in issues))
        self.assertEqual(pipeline.load('incremental', package, self.db)['status'], 'already_processed')

    def test_conflicts_rejections_and_blocked_identity(self):
        self.incremental()
        self.assertEqual(self.rows("SELECT cpu_time_ms, has_source_conflict FROM fact_query_execution WHERE query_id='q001'"), [(60000, True)])
        self.assertEqual(self.rows("SELECT COUNT(*) FROM fact_query_execution WHERE query_id IN ('q107','q108','q109')"), [(0,)])
        self.assertEqual(self.rows('SELECT issue_type, COUNT(*) FROM etl_issue GROUP BY issue_type ORDER BY issue_type'),
                         [('CONFLICT', 3), ('INVALID_METRIC', 2), ('REJECTED', 2)])
        self.assertTrue(all(row[0] for row in self.rows('SELECT raw_payload FROM etl_issue')))
        package = self.later_package()
        records = [json.loads(line) for line in (package / 'events.jsonl').read_text().splitlines()]
        record = copy.deepcopy(next(item for item in records if item['raw_record_id'] == 'r107-a'))
        record.update(raw_record_id='r107-later', raw_ingested_at='2026-03-01T01:00:00Z')
        self.append_record(package, record)
        pipeline.load('incremental', package, self.db)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM fact_query_execution WHERE query_id='q107'"), [(0,)])

    def test_replay_preserves_all_rows_and_later_duplicate_preserves_audit(self):
        self.incremental()
        before = self.snapshot()
        self.assertEqual(self.incremental()['status'], 'already_processed')
        self.assertEqual(self.snapshot(), before)
        package = self.later_package()
        record = json.loads((package / 'events.jsonl').read_text().splitlines()[2])
        record.update(raw_record_id='r002-later', raw_ingested_at='2026-03-01T01:00:00Z')
        self.append_record(package, record)
        result = pipeline.load('incremental', package, self.db)
        self.assertEqual(result['duplicates_skipped'], 1)
        self.assertEqual(self.snapshot(False), {key: value for key, value in before.items() if not key.startswith('etl_')})

    def test_transaction_rolls_back_dimensions_issues_and_checkpoint(self):
        before = self.snapshot()
        with patch.object(pipeline, 'insert_fact', side_effect=RuntimeError('simulated interruption')):
            with self.assertRaisesRegex(RuntimeError, 'simulated interruption'):
                self.incremental()
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.incremental()['facts_inserted'], 7)

    def test_registry_correction_reassigns_facts_without_rewriting_bridge(self):
        self.incremental()
        old_bridge = self.rows('SELECT * FROM bridge_query_table ORDER BY ALL')
        package = self.later_package()
        path = package / 'user_team_membership.csv'
        import csv
        with path.open(encoding='utf-8', newline='') as handle:
            reader = csv.DictReader(handle)
            fieldnames, rows = reader.fieldnames, list(reader)
        for row in rows:
            if row['user_id'] == 'U_ANA':
                row['valid_to' if row['team_id'] == 'T_ANALYTICS' else 'valid_from'] = '2026-01-17T00:00:00Z'
                row['updated_at'] = '2026-03-01T01:00:00Z'
        with path.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        result = pipeline.load('incremental', package, self.db)
        self.assertEqual(result['facts_reassigned'], 2)
        self.assertEqual(self.rows('''SELECT t.team_id FROM fact_query_execution f JOIN dim_team t USING(team_key)
            WHERE query_id='q003' '''), [('T_PLATFORM',)])
        self.assertEqual(self.rows('SELECT * FROM bridge_query_table ORDER BY ALL'), old_bridge)
        self.assertEqual(self.rows('SELECT SUM(cpu_time_ms) FROM fact_query_execution'), [(175001.0,)])
        # Logical FK deliberately used for bridge -> mutable fact (DuckDB limitation).
        self.assertEqual(self.rows('''SELECT COUNT(*) FROM bridge_query_table b
            LEFT JOIN fact_query_execution f USING(query_key) WHERE f.query_key IS NULL'''), [(0,)])


if __name__ == '__main__':
    unittest.main()
