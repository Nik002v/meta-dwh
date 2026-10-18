# Meta DWH — local demonstration

An internship selection task solution for historical analysis of table usage and reported Trino query resource consumption. DuckDB stores the warehouse, Python and SQL populate it, and two cumulative packages of JSONL/CSV files simulate the sources. No Trino cluster, AWS account, SQLite database or separate registry service is required.

**The DuckDB database `warehouse/meta_dwh.duckdb` is generated locally and is not stored in Git.** The repository includes the code, both synthetic data packages and sample results in `outputs/`. The design and assumptions are documented in English as part of the task submission.

**Start here: [Quick overview](docs/QUICK_OVERVIEW.md)** — the model, design choices, resource metrics, assumptions, required metadata and update process in one short document.

[Key assumptions](docs/DESIGN.md#key-assumptions) identifies the simplifying assumptions, especially the historical registry and source ingestion/modification timestamps. Their availability in the actual system has not been confirmed.

[Design rationale](docs/DESIGN.md#design-rationale) briefly explains the choices of model, grain, metrics and loading strategy.

## Getting started

Requires Python 3.10+; verified with Python 3.12 and DuckDB 1.4.3. From this directory, in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --disable-pip-version-check -r requirements.txt
```

After cloning, create and populate the database, check that reprocessing is safe, and generate the reports:

```powershell
.\.venv\Scripts\python.exe pipeline.py init
.\.venv\Scripts\python.exe pipeline.py incremental
.\.venv\Scripts\python.exe pipeline.py incremental
.\.venv\Scripts\python.exe pipeline.py report
```

The second `incremental` run returns `already_processed` and leaves the rows unchanged. `init` refuses to overwrite an existing database. On Linux/macOS, replace `.\.venv\Scripts\python.exe` with `.venv/bin/python`.

If the database already exists, use it with `incremental` and `report`. To start a fresh demonstration, add a new path, such as `--db warehouse/demo.duckdb`, to every load and report command.

When opening the database manually, use `SET TimeZone = 'UTC';` to display timestamps as the pipeline does. Otherwise, a DuckDB client may display the same instant in the local time zone.

The data packages are already included. To regenerate them deterministically and run the tests:

```powershell
.\.venv\Scripts\python.exe generate_data.py
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Regeneration restores the original synthetic files; it does not modify the database. Choose a new database file for a fresh demonstration afterward.

## Repository contents

| Path | Contents |
|---|---|
| [docs/QUICK_OVERVIEW.md](docs/QUICK_OVERVIEW.md) | Short overview of the model, metrics, assumptions and update process |
| [docs/DESIGN.md](docs/DESIGN.md) | Model, grain, relationships, measures, assumptions and loading rules |
| [docs/SYNTHETIC_DATA.md](docs/SYNTHETIC_DATA.md) | The two packages, example scenarios and expected results |
| [sql/schema.sql](sql/schema.sql) | Definitions of dimensions, fact, bridge and operational tables |
| [pipeline.py](pipeline.py) | Normalization, enrichment, initial/incremental loads and transactional writes |
| [sql/reports/](sql/reports/) | Executable example queries; report parameters are defined at the top of the relevant queries |
| [generate_data.py](generate_data.py) | Generator for both packages |
| [data/init/](data/init/) and [data/incremental/](data/incremental/) | Simulated sources, including the external registry |
| [tests/test_pipeline.py](tests/test_pipeline.py) | Checks for business results, reprocessing and interrupted loads |
| `warehouse/meta_dwh.duckdb` | Locally generated DWH; not versioned |
| `outputs/*.csv` | Results of the five SQL reports |
| [outputs/validation.txt](outputs/validation.txt) | Results of the final validation run |

## Expected results after both packages

- 14 executions, 19 bridge links, 5 accounts, 3 teams, 4 tables and 9 SQL texts.
- A total of 175001 ms of known CPU time and 11000 reported input bytes. Two executions have unknown CPU and input bytes.
- For `feature_events`: 4 distinct accounts with an INPUT link, including failed executions and ZERO/UNKNOWN read evidence.
- Team `T_PLATFORM`: 7 queries, including 1 failed query, 90 CPU seconds and 5900 input bytes.
- `etl_issue`: 3 conflict records, 2 rejected records and 2 invalid metrics. The accepted `q001` retains its 60000 ms CPU value and is flagged as having a conflict.

Results reflect known reported metrics; `NULL` does not mean zero. There is no `Unknown` team: registry completeness and correctness are documented assumptions.
