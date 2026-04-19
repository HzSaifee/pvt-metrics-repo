CREATE TABLE IF NOT EXISTS cdt.{{ table_name }} (
  account_id     VARCHAR    COMMENT 'Salesforce Account ID. Snowflake is the source of truth for casing/format.',
  source         VARCHAR    COMMENT 'Origin at first insert: snowflake, redshift, or both',
  first_seen_at  TIMESTAMP  COMMENT 'Pacific time when the DAG first inserted this account',
  snapshot_date  DATE       COMMENT 'Pacific date when the DAG first inserted this account'
)
WITH (
  format = 'PARQUET'
)
