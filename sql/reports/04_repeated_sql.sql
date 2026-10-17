-- Hash identifies exact SQL text, not an execution or all session semantics.
SELECT q.sql_hash, q.sql_text, COUNT(*) AS executions,
       SUM(f.cpu_time_ms) / 1000.0 AS cpu_seconds,
       AVG(f.wall_time_ms) AS avg_wall_time_ms
FROM fact_query_execution f
JOIN dim_query_text q USING (query_text_key)
GROUP BY q.sql_hash, q.sql_text
ORDER BY executions DESC, cpu_seconds DESC NULLS LAST;
