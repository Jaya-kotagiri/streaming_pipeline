-- ============================================================
-- file_change_events table - Snowflake (primary prod target)
-- ============================================================
CREATE TABLE IF NOT EXISTS file_change_events (
    event_id        STRING        PRIMARY KEY,
    file_name       STRING        NOT NULL,
    old_value       STRING,
    new_value       STRING,
    event_type      STRING        NOT NULL,   -- INSERT | UPDATE | DELETE
    line_number     INTEGER,
    event_timestamp TIMESTAMP_NTZ NOT NULL,
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- SQL Server variant
-- ============================================================
IF OBJECT_ID('dbo.file_change_events', 'U') IS NOT NULL
    DROP TABLE dbo.file_change_events;
GO

CREATE TABLE dbo.file_change_events (
    event_id        VARCHAR(36)   NOT NULL PRIMARY KEY,
    file_name       VARCHAR(255)  NOT NULL,
    old_value       NVARCHAR(MAX),
    new_value       NVARCHAR(MAX),
    event_type      VARCHAR(20)   NOT NULL,
    line_number     INT,
    event_timestamp DATETIME2     NOT NULL,
    loaded_at       DATETIME2     DEFAULT SYSUTCDATETIME()
);
GO

-- ============================================================
-- Postgres variant (JDBC fallback target)
-- ============================================================
CREATE TABLE IF NOT EXISTS file_change_events (
    event_id        UUID          PRIMARY KEY,
    file_name       VARCHAR(255)  NOT NULL,
    old_value       TEXT,
    new_value       TEXT,
    event_type      VARCHAR(20)   NOT NULL,
    line_number     INTEGER,
    event_timestamp TIMESTAMP     NOT NULL,
    loaded_at       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);
