-- Migration 001: add app_hints table
-- Application-level key/value hint store for persisting operational watermarks
-- across restarts. Values are text; date hints use YYYY-MM-DD format.
CREATE TABLE IF NOT EXISTS app_hints (
    key        text PRIMARY KEY,
    value      text,
    updated_at timestamptz NOT NULL DEFAULT NOW()
);
