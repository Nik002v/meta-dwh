-- Parameters: table identity and UTC completion interval [from_ts, to_ts).
-- Count accounts with a reported INPUT link, including failed executions
-- and zero/unknown reported reads. No query-state or read-evidence filter.
WITH parameters AS (
    SELECT 'iceberg' AS catalog_name, 'product_analytics' AS schema_name,
           'feature_events' AS table_name,
           TIMESTAMPTZ '2026-01-01 00:00:00+00' AS from_ts,
           TIMESTAMPTZ '2026-03-01 00:00:00+00' AS to_ts
)
SELECT COUNT(DISTINCT f.user_key) AS distinct_accounts
FROM fact_query_execution f
JOIN bridge_query_table b USING (query_key)
JOIN dim_table t USING (table_key)
CROSS JOIN parameters p
WHERE t.catalog_name = p.catalog_name AND t.schema_name = p.schema_name
  AND t.table_name = p.table_name AND b.access_role = 'INPUT'
  AND f.completed_at >= p.from_ts AND f.completed_at < p.to_ts;
