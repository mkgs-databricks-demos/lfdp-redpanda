CREATE OR REFRESH STREAMING TABLE deleted_records_from_profiles_bronze;

CREATE FLOW deleted_records AS 
INSERT INTO deleted_records_from_profiles_bronze BY NAME 
SELECT * FROM (
  FROM STREAM(profiles_cdf) |>
  WHERE (_change_type = 'delete') |>
  SELECT *
    ,_change_type AS change_type
    ,_commit_version as commit_version
    ,_commit_timestamp as commit_timestamp |>
  SELECT * EXCEPT (_change_type, _commit_version, _commit_timestamp)
);

CREATE OR REFRESH MATERIALIZED VIEW mv_deleted_profile_rcrd_cnt AS (
  SELECT COUNT(*) AS cnt FROM deleted_records_from_profiles_bronze
);