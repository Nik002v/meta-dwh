-- Current team labels, historical responsibility stored on each execution.
SELECT d.calendar_date, t.team_id, t.team_name, COUNT(*) AS query_count,
       SUM(f.cpu_time_ms) / 1000.0 AS cpu_seconds,
       SUM(f.physical_input_bytes) AS physical_input_bytes
FROM fact_query_execution f
JOIN dim_date d ON d.date_key = f.completed_date_key
JOIN dim_team t USING (team_key)
WHERE f.completed_at >= TIMESTAMPTZ '2026-01-01 00:00:00+00'
  AND f.completed_at < TIMESTAMPTZ '2026-03-01 00:00:00+00'
GROUP BY d.calendar_date, t.team_id, t.team_name
ORDER BY d.calendar_date, t.team_id;
