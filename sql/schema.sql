-- All timestamps are UTC. Descriptive dimensions use SCD Type 1.
CREATE SEQUENCE user_keys START 1;
CREATE SEQUENCE team_keys START 1;
CREATE SEQUENCE table_keys START 1;
CREATE SEQUENCE text_keys START 1;
CREATE SEQUENCE query_keys START 1;

CREATE TABLE dim_user (
    user_key BIGINT PRIMARY KEY DEFAULT nextval('user_keys'),
    user_id VARCHAR UNIQUE NOT NULL,
    username VARCHAR UNIQUE NOT NULL,
    display_name VARCHAR NOT NULL,
    account_type VARCHAR NOT NULL CHECK (account_type IN ('HUMAN', 'SERVICE', 'SHARED')),
    is_active BOOLEAN NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    dwh_inserted_at TIMESTAMPTZ NOT NULL,
    dwh_updated_at TIMESTAMPTZ NOT NULL,
    load_batch_id VARCHAR NOT NULL
);

CREATE TABLE dim_team (
    team_key BIGINT PRIMARY KEY DEFAULT nextval('team_keys'),
    team_id VARCHAR UNIQUE NOT NULL,
    team_name VARCHAR NOT NULL,
    is_active BOOLEAN NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    dwh_inserted_at TIMESTAMPTZ NOT NULL,
    dwh_updated_at TIMESTAMPTZ NOT NULL,
    load_batch_id VARCHAR NOT NULL
);

CREATE TABLE dim_table (
    table_key BIGINT PRIMARY KEY DEFAULT nextval('table_keys'),
    catalog_name VARCHAR NOT NULL,
    schema_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    dwh_inserted_at TIMESTAMPTZ NOT NULL,
    load_batch_id VARCHAR NOT NULL,
    UNIQUE (catalog_name, schema_name, table_name)
);

CREATE TABLE dim_query_text (
    query_text_key BIGINT PRIMARY KEY DEFAULT nextval('text_keys'),
    sql_hash VARCHAR UNIQUE NOT NULL,
    sql_text VARCHAR NOT NULL,
    dwh_inserted_at TIMESTAMPTZ NOT NULL,
    load_batch_id VARCHAR NOT NULL
);

CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    calendar_date DATE UNIQUE NOT NULL,
    year SMALLINT NOT NULL,
    month TINYINT NOT NULL,
    week_start_date DATE NOT NULL,
    day_of_week TINYINT NOT NULL
);

CREATE TABLE fact_query_execution (
    query_key BIGINT PRIMARY KEY DEFAULT nextval('query_keys'),
    cluster_id VARCHAR NOT NULL,
    query_id VARCHAR NOT NULL,
    user_key BIGINT NOT NULL REFERENCES dim_user(user_key),
    team_key BIGINT NOT NULL REFERENCES dim_team(team_key),
    completed_date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    query_text_key BIGINT NOT NULL REFERENCES dim_query_text(query_text_key),
    created_at TIMESTAMPTZ NOT NULL,
    execution_started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ NOT NULL,
    cpu_time_ms DOUBLE CHECK (cpu_time_ms >= 0),
    physical_input_bytes BIGINT CHECK (physical_input_bytes >= 0),
    peak_user_memory_bytes BIGINT CHECK (peak_user_memory_bytes >= 0),
    wall_time_ms DOUBLE CHECK (wall_time_ms >= 0),
    queued_time_ms DOUBLE CHECK (queued_time_ms >= 0),
    written_bytes BIGINT CHECK (written_bytes >= 0),
    written_rows BIGINT CHECK (written_rows >= 0),
    spilled_bytes BIGINT CHECK (spilled_bytes >= 0),
    query_state VARCHAR NOT NULL CHECK (query_state IN ('FINISHED', 'FAILED')),
    query_type VARCHAR,
    source VARCHAR,
    resource_group_id VARCHAR,
    retry_policy VARCHAR,
    error_code VARCHAR,
    statistics_complete BOOLEAN,
    trino_version VARCHAR,
    transaction_id VARCHAR,
    has_invalid_metrics BOOLEAN NOT NULL,
    has_source_conflict BOOLEAN NOT NULL DEFAULT false,
    source_event_hash VARCHAR NOT NULL,
    source_raw_record_id VARCHAR NOT NULL,
    source_ingested_at TIMESTAMPTZ NOT NULL,
    dwh_inserted_at TIMESTAMPTZ NOT NULL,
    dwh_updated_at TIMESTAMPTZ NOT NULL,
    load_batch_id VARCHAR NOT NULL,
    UNIQUE (cluster_id, query_id),
    CHECK (completed_at >= created_at)
);

CREATE TABLE bridge_query_table (
    -- Logical FK to fact.query_key: checked by the loader and validation suite.
    -- DuckDB's indexed-update FK limitation otherwise prevents correcting a
    -- fact's team assignment while bridge rows reference that fact.
    query_key BIGINT NOT NULL,
    table_key BIGINT NOT NULL REFERENCES dim_table(table_key),
    access_role VARCHAR NOT NULL CHECK (access_role IN ('INPUT', 'WRITE_TARGET')),
    read_evidence VARCHAR NOT NULL CHECK (read_evidence IN ('POSITIVE', 'ZERO', 'UNKNOWN', 'NOT_APPLICABLE')),
    dwh_inserted_at TIMESTAMPTZ NOT NULL,
    load_batch_id VARCHAR NOT NULL,
    PRIMARY KEY (query_key, table_key, access_role),
    CHECK ((access_role = 'WRITE_TARGET' AND read_evidence = 'NOT_APPLICABLE')
        OR (access_role = 'INPUT' AND read_evidence <> 'NOT_APPLICABLE'))
);

-- Technical tables, not extra business facts/dimensions.
CREATE TABLE etl_checkpoint (
    source_name VARCHAR PRIMARY KEY,
    processed_until TIMESTAMPTZ NOT NULL
);

CREATE TABLE etl_load (
    batch_id VARCHAR PRIMARY KEY,
    package_id VARCHAR NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    processed_until TIMESTAMPTZ NOT NULL,
    summary_json JSON NOT NULL
);

CREATE TABLE etl_issue (
    issue_id VARCHAR PRIMARY KEY,
    batch_id VARCHAR NOT NULL,
    raw_record_id VARCHAR,
    cluster_id VARCHAR,
    query_id VARCHAR,
    issue_type VARCHAR NOT NULL CHECK (issue_type IN ('REJECTED', 'CONFLICT', 'INVALID_METRIC')),
    reason VARCHAR NOT NULL,
    raw_payload VARCHAR NOT NULL,
    dwh_inserted_at TIMESTAMPTZ NOT NULL
);
