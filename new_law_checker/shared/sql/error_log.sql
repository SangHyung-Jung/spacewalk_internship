CREATE TABLE error_log (
  table_nm varchar(64),
  id varchar(64),
  message varchar(256),
  created_dt date
);
CREATE INDEX idx_error_log_table_nm_id ON error_log (table_nm, id);