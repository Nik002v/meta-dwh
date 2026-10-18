# Synthetic source snapshots

The committed files are reproducible with `python generate_data.py`. SQL texts and counters are illustrative; these business queries are not executed against a real Trino cluster. The same input files always produce the same business results, although warehouse load timestamps and batch UUIDs change on a fresh rebuild.

Each package has `manifest.json`, `events.jsonl`, `users_history.csv`, `teams_history.csv`, and `user_team_membership.csv`. CSV histories are external registry data, not warehouse history tables. Snapshot 2 retains snapshot 1's event records and original ingestion timestamps.

| Package | Exclusive cutoff | Raw records | New accepted facts |
|---|---|---:|---:|
| `data/init` | 2026-02-01 00:00 UTC | 9 | 7 |
| `data/incremental` | 2026-03-01 00:00 UTC | 22 total; 13 selected incrementally | 7 |

## Initial snapshot

| Query / record | Purpose |
|---|---|
| Created event for `q001` | Demonstrates that query creation does not add another execution |
| `q001` | Ana reads feature events and users; 60000 ms CPU versus 10000 ms wall |
| `q002` | Boris executes exactly the same SQL under a different query ID |
| `q003` | Failed SELECT still consumes 5000 ms CPU and 100 input bytes |
| `q004` | Service account reads feature events and writes daily usage; one input and one target |
| `q005` | SELECT 1: a valid fact with no table bridge rows |
| `q006` | Self-join: repeated feature-events inputs collapse into one INPUT link |
| `q007` | Reporting service: missing CPU/input counters, incomplete statistics, UNKNOWN read evidence |
| `r001-copy` | Exact copy of `q001` with another ingestion envelope; ignored as a duplicate |

Initial registry: Ana and reporting service belong to T_ANALYTICS; Boris and ETL service belong to T_PLATFORM. Initial totals: 7 executions, 9 bridge rows, 108001 ms known CPU and 6800 known input bytes.

## Incremental changes

- Ana transfers to T_PLATFORM effective February 1. The old membership interval is closed and its `updated_at` changes; a new interval is inserted.
- Analytics is renamed to Product Analytics on February 2. The stable team ID and DWH team key remain unchanged.
- Ana's display name changes to Ana Jovanovic. The account identity/username remain unchanged.
- A new Research team and user Mila are inserted. Mila belongs to Research.

| Query / record | Purpose |
|---|---|
| `q008` / `r008-late` | January 15 query received February 3; remains attributed to Analytics |
| `q101` | Ana's February query belongs to Platform; third execution of the original join SQL |
| `q102` | Published exactly at the previous cutoff; successful Boris query with ZERO read evidence; belongs to January reporting |
| `q103` | Failed INSERT by Ana; retains reported writer work without asserting committed changes |
| `q104` | UPDATE by ETL service: daily_usage is both INPUT and WRITE_TARGET |
| `q105` | Mila reads a newly observed table, iceberg.billing.invoices |
| `q106` | Negative CPU and input-byte counters become NULL; two metric issues, valid remaining query data retained |
| `r107-a`, `r107-b` | Conflicting copies of previously unseen q107; both logged, no fact |
| `r001-conflict` | Different CPU for existing q001; original 60000 ms retained and flagged |
| `r002-copy` | Exact duplicate received later; no fact/audit modification |
| `r108-bad-time` | End before creation; rejected |
| `r109-no-id` | Missing query ID; rejected |

## Expected final business results

| Metric | Value |
|---|---:|
| Facts | 14 |
| Bridge rows | 19 |
| Users / teams / tables / SQL texts | 5 / 3 / 4 / 9 |
| Failed executions included in usage | 2 |
| Known CPU milliseconds | 175001 |
| Known physical input bytes | 11000 |
| Queries with NULL CPU / input bytes | 2 / 2 |
| Queries with invalid metrics / source conflict | 1 / 1 |
| Conflict issues / rejected records / invalid-metric issues | 3 / 2 / 2 |
| Distinct accounts with an INPUT link to feature_events, regardless of execution state/read evidence | 4 |

| Responsible team | Executions | Known CPU ms | Known input bytes |
|---|---:|---:|---:|
| T_ANALYTICS, now Product Analytics | 6 | 76001 | 4200 |
| T_PLATFORM | 7 | 90000 | 5900 |
| T_RESEARCH | 1 | 9000 | 900 |

Both completed-failure resources and valid parts of q106 are included. Aggregates over NULL metrics describe the known reported portion; the quality report exposes the unknown counts. q001 remains included using its accepted original values with a conflict flag. The repeated join text has 3 executions and 110000 ms CPU.

The automated tests additionally create temporary later snapshots to verify that a previously conflicted identity cannot silently reappear, an exact replay preserves audits, a later duplicate changes no business rows, and corrected registry membership can reassign existing facts. A numeric-overflow fixture supplies `1e309` as query-level and per-input byte counters: both become NULL and are logged, the valid execution is retained with UNKNOWN read evidence, and replay remains safe. These temporary packages are test fixtures, not additional committed source packages; the expected totals above are unchanged.
