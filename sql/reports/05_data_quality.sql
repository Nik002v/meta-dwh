SELECT COUNT(*) AS accepted_executions,
       COUNT(*) FILTER (WHERE query_state = 'FAILED') AS failed_executions,
       SUM(cpu_time_ms) AS reported_cpu_ms,
       SUM(physical_input_bytes) AS reported_input_bytes,
       COUNT(*) FILTER (WHERE cpu_time_ms IS NULL) AS unknown_cpu_queries,
       COUNT(*) FILTER (WHERE physical_input_bytes IS NULL) AS unknown_input_queries,
       COUNT(*) FILTER (WHERE has_invalid_metrics) AS queries_with_invalid_metrics,
       COUNT(*) FILTER (WHERE has_source_conflict) AS queries_with_source_conflict,
       (SELECT COUNT(*) FROM etl_issue WHERE issue_type = 'REJECTED') AS rejected_records,
       (SELECT COUNT(*) FROM etl_issue WHERE issue_type = 'CONFLICT') AS conflicting_records,
       (SELECT COUNT(*) FROM etl_issue WHERE issue_type = 'INVALID_METRIC') AS invalid_metric_issues
FROM fact_query_execution;
