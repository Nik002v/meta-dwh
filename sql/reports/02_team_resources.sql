-- Includes successful AND failed queries. Never join the bridge to sum query CPU.
-- Aggregates use available (non-NULL) metrics; quality diagnostics are in report 05.
WITH parameters AS (
    SELECT 'T_PLATFORM' AS team_id,
           TIMESTAMPTZ '2026-01-01 00:00:00+00' AS from_ts,
           TIMESTAMPTZ '2026-03-01 00:00:00+00' AS to_ts
)
SELECT t.team_id, t.team_name, COUNT(*) AS query_count,
       COUNT(*) FILTER (WHERE f.query_state = 'FAILED') AS failed_queries,
       SUM(f.cpu_time_ms) / 1000.0 AS cpu_seconds,
       SUM(f.physical_input_bytes) AS physical_input_bytes,
       SUM(f.written_bytes) AS logical_writer_input_bytes,
       SUM(f.spilled_bytes) AS spilled_bytes,
       MAX(f.peak_user_memory_bytes) AS max_single_query_peak_user_memory_bytes,
       AVG(f.wall_time_ms) AS avg_wall_time_ms,
       QUANTILE_CONT(f.wall_time_ms, 0.95) AS p95_wall_time_ms
FROM fact_query_execution f
JOIN dim_team t USING (team_key)
CROSS JOIN parameters p
WHERE t.team_id = p.team_id
  AND f.completed_at >= p.from_ts AND f.completed_at < p.to_ts
GROUP BY t.team_id, t.team_name;
