-- Create and populate the target table.
CREATE OR REFRESH STREAMING TABLE profiles_scd2
(
  email STRING,
  first_name STRING,
  last_name STRING,
  last_login TIMESTAMP_LTZ,
  registration_date DATE,
  preferences STRUCT<language: STRING, notifications: STRING>,
  subscription_level STRING,
  user_id STRING
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.variantType-preview' = 'supported',
  'quality' = 'silver'
);

CREATE FLOW profiles_cdc_scd2 AS AUTO CDC INTO
  profiles_scd2
FROM (
  FROM STREAM(profiles_cdf) |>
  SELECT *, parse_json(value_str) as variant_col |>
  SELECT 
    topic
    ,partition
    ,offset
    ,timestamp
    ,timestampType
    ,ingestTime
    ,variant_col:email::string as email
    ,variant_col:first_name::string as first_name
    ,variant_col:last_name::string as last_name
    ,variant_col:last_login::timestamp as last_login
    ,variant_col:registration_date::date as registration_date
    ,variant_col:preferences::struct<language: string, notifications: string> as preferences
    ,variant_col:subscription_level::string as subscription_level
    ,variant_col:user_id::string as user_id
    ,_change_type
    ,_commit_version
    ,_commit_timestamp
)
KEYS
  (user_id)
APPLY AS DELETE WHEN
  _change_type = "delete"
SEQUENCE BY
  (timestamp)
COLUMNS * EXCEPT
  (_change_type, _commit_version, _commit_timestamp, topic, partition, offset, timestamp, timestampType, ingestTime)
STORED AS
  SCD TYPE 2;
