-- =============================================================================
-- PostgreSQL init script
-- Chạy tự động một lần khi container khởi tạo lần đầu.
--
-- PostgreSQL này phục vụ 2 hệ thống:
--   1. iceberg_catalog  — JDBC catalog cho Iceberg (Spark + Trino)
--   2. dagster          — Metadata storage cho Dagster webserver + daemon
--
-- Database "iceberg_catalog" đã được tạo sẵn bởi biến môi trường
-- POSTGRES_DB=iceberg_catalog trong docker-compose.yml.
-- Script này tạo thêm database "dagster" và cấp đủ quyền.
-- =============================================================================

-- ── 1. Dagster metadata database ─────────────────────────────────────────────
CREATE DATABASE dagster OWNER iceberg;
GRANT ALL PRIVILEGES ON DATABASE dagster TO iceberg;

-- ── 2. Iceberg JDBC catalog — cấp quyền trên iceberg_catalog ─────────────────
-- (database đã tồn tại, chỉ cần đảm bảo user "iceberg" có full quyền)
GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO iceberg;

-- ── 3. Connect vào iceberg_catalog để tạo schema ─────────────────────────────
\connect iceberg_catalog

-- Đảm bảo user iceberg sở hữu schema public
ALTER SCHEMA public OWNER TO iceberg;
GRANT ALL ON SCHEMA public TO iceberg;

-- Iceberg JDBC catalog sẽ tự tạo 2 bảng này khi Spark/Trino kết nối lần đầu:
--   - iceberg_tables            (lưu metadata table: location, format, schema...)
--   - iceberg_namespace_properties  (lưu properties của database/namespace)
--
-- Tạo trước để đảm bảo không có race condition khi nhiều service cùng start:
CREATE TABLE IF NOT EXISTS iceberg_tables (
    catalog_name            VARCHAR(255) NOT NULL,
    table_namespace         VARCHAR(255) NOT NULL,
    table_name              VARCHAR(255) NOT NULL,
    metadata_location       VARCHAR(1000),
    previous_metadata_location VARCHAR(1000),
    CONSTRAINT iceberg_tables_pk PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
    catalog_name            VARCHAR(255) NOT NULL,
    namespace               VARCHAR(255) NOT NULL,
    property_key            VARCHAR(255),
    property_value          VARCHAR(1000),
    CONSTRAINT iceberg_namespace_properties_pk PRIMARY KEY (catalog_name, namespace, property_key)
);

GRANT ALL PRIVILEGES ON TABLE iceberg_tables TO iceberg;
GRANT ALL PRIVILEGES ON TABLE iceberg_namespace_properties TO iceberg;

-- ── 4. Connect vào dagster để cấp quyền schema ───────────────────────────────
\connect dagster

ALTER SCHEMA public OWNER TO iceberg;
GRANT ALL ON SCHEMA public TO iceberg;
-- Dagster sẽ tự tạo toàn bộ tables của nó (runs, events, schedules, v.v.)
-- khi dagster-webserver khởi động lần đầu.

