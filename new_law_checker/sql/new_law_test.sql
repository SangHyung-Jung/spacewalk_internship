CREATE TABLE new_law_test (
  law_type varchar(64),
  law_nm varchar(256),
  enf_dt varchar(64),
  pub_no varchar(256),
  pub_dt varchar(64),
  redev_type varchar(64),
  is_renewal varchar(64),
  checked_dt date,
  url varchar(256)
);
CREATE INDEX idx_new_law_test_law_type ON new_law_test (law_type);
