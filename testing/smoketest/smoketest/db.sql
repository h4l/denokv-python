PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE migration_state(
  k integer not null primary key,
  version integer not null
);
INSERT INTO migration_state VALUES(0,3);
CREATE TABLE data_version (
  k integer primary key,
  version integer not null
, seq integer not null default 0);
INSERT INTO data_version VALUES(0,6,0);
CREATE TABLE kv (
  k blob primary key,
  v blob not null,
  v_encoding integer not null,
  version integer not null
, seq integer not null default 0, expiration_ms integer not null default -1) without rowid;
INSERT INTO kv VALUES(X'02736d6f6b657465737400',X'ff0f6f22076d657373616765220b48656c6c6f20576f726c647b01',1,6,0,-1);
CREATE TABLE queue (
  ts integer not null,
  id text not null,
  data blob not null,
  backoff_schedule text not null,
  keys_if_undelivered blob not null,

  primary key (ts, id)
);
CREATE TABLE queue_running(
  deadline integer not null,
  id text not null,
  data blob not null,
  backoff_schedule text not null,
  keys_if_undelivered blob not null,

  primary key (deadline, id)
);
CREATE INDEX kv_expiration_ms_idx on kv (expiration_ms);
COMMIT;
