CREATE OR REFRESH STREAMING TABLE deleted_records_from_profiles_bronze;

CREATE FLOW deleted_records AS 
INSERT INTO deleted_records_from_profiles_bronze BY NAME 
SELECT * FROM (
  FROM STREAM(profiles_cdf) |>
  WHERE `_change_type` = 'delete'
);