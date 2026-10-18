# Meta DWH — quick overview

This warehouse answers **who queried a table, how much a team consumed, and how usage changed over time**. Since the task provides no source schema or infrastructure access, the implementation uses synthetic JSONL event exports and CSV identity metadata, with Python transformations and a persistent DuckDB warehouse. JSONL stands in for exported Trino events stored in S3/Iceberg; it is not an Iceberg implementation. DuckDB makes the solution runnable locally.

## Model and design choices

The model is a star schema with a bridge for queries accessing multiple tables.

| Table | Grain and purpose |
|---|---|
| `fact_query_execution` | One accepted completed execution, unique by `(cluster_id, query_id)`; stores resource measures, timestamps, state and dimension keys. Includes successful and failed executions. |
| `dim_user` | One stable account, including human, service and shared accounts. |
| `dim_team` | One stable team, with its current name and active status. |
| `dim_table` | One observed `(catalog_name, schema_name, table_name)`. |
| `dim_query_text` | One exact SQL text, identified by SHA-256; repeated SQL does not imply duplicate executions. |
| `dim_date` | One observed UTC completion date. |
| `bridge_query_table` | One `(query_key, table_key, access_role)`; links executions to tables as `INPUT` or `WRITE_TARGET`. |

Each fact references one user, team, date and SQL text. The bridge represents the many-to-many relationship between executions and tables, including tables from different catalogs. Repeated references in a self-join collapse into one link per table/role. **Resources remain on the execution fact and are counted once; they are not allocated across tables.** Team resource reports therefore do not join the bridge.

Execution detail supports exact distinct-account counts for any period and historical trends without requiring precomputed aggregates. User/team descriptive attributes use Type 1 updates (replace old labels), while historical team responsibility is stored on each fact.

## Resource metrics and reporting rules

| Measures | Meaning and aggregation |
|---|---|
| `cpu_time_ms`, `physical_input_bytes` | Reported compute work and physical input volume; sum once per execution, including failures. |
| `written_bytes`, `written_rows`, `spilled_bytes` | Writer-operator input and spill volume; sum. Writer counters do not establish committed changes or physical S3 growth. |
| `peak_user_memory_bytes` | Per-query peak user memory; use max/percentiles. Summing peaks does not measure simultaneous team memory demand. |
| `wall_time_ms`, `queued_time_ms` | Reported elapsed and queue time; use averages/percentiles for latency. |

Missing or invalid metrics remain `NULL`; invalid metrics are logged. Totals describe known reported consumption, not monetary cost. Reporting uses UTC completion intervals `[from, to)`: the whole execution belongs to its completion period, while its responsible team is determined at creation time.

“Distinct users” means distinct accounts with a reported `INPUT` link, including failed executions and zero/unknown reads. An input link does not guarantee positive physical I/O. Service accounts count as accounts, not individual people.

## Assumptions and additional metadata

These are explicit simplifications, not claims about the actual infrastructure:

- Completed events supply query identity, account, lifecycle times, statistics and structured input/output table references. Table links come from event metadata, without parsing SQL. Final events are treated as immutable observations.
- Account IDs/usernames are stable. Table names are not renamed or reused for different objects during the demonstration.
- A complete, authoritative historical registry maps every account to exactly one responsible team at query creation time, including service accounts. Missing or ambiguous mappings abort the batch.
- Event exports provide reliable `raw_ingested_at`; registry records provide `updated_at` on every change. Each export guarantees complete publication below its cutoff: later exports cannot introduce changes timestamped behind an already processed boundary.

The external registry is simulated by three CSV files:

| File | Business fields |
|---|---|
| `users_history.csv` | `user_id`, `trino_username`, `display_name`, `account_type`, `is_active` |
| `teams_history.csv` | `team_id`, `team_name`, `is_active` |
| `user_team_membership.csv` | `user_id`, `team_id` |

Every file also has `valid_from`, `valid_to` and `updated_at`. Validity is `[valid_from, valid_to)`, with an empty end meaning open-ended. ETL resolves the account and membership at query creation time, then stores the resulting team key. Ordinary transfers preserve previous assignments; membership corrections trigger reassignment of affected historical facts. Registry history stays outside the DWH; current labels populate the dimensions.

## How data is updated

`init` loads the initial snapshot. Each subsequent `incremental` run selects new events by ingestion time and registry changes by modification time in `[previous_cutoff, new_cutoff)`. This captures late arrivals even when their queries finished in an earlier period. New dimensions, facts and bridge links are inserted; changed profiles are updated.

Exact duplicates are skipped. Conflicting copies are logged: an existing fact is retained and flagged; a new identity with conflicting copies is blocked pending explicit resolution. Dimensions, facts, issues, load audit and source checkpoints commit in one transaction. Reprocessing the same package changes nothing. Repeated runs can be scheduled as exports become available; this demo invokes them through the CLI.

## Implementation and example queries

- [Schema](../sql/schema.sql) and [Python transformations](../pipeline.py): create and populate the warehouse.
- [Distinct users of table X](../sql/reports/01_distinct_users.sql): `COUNT(DISTINCT user_key)` through INPUT links for the selected table and period.
- [Resources consumed by team Y](../sql/reports/02_team_resources.sql): aggregate execution metrics directly by responsible team and period.
- [Daily team history](../sql/reports/03_daily_team_history.sql) and [data quality](../sql/reports/05_data_quality.sql): trends and missing/conflicting data.

See [README](../README.md) for running instructions and [DESIGN](DESIGN.md) for detailed semantics and limitations.
