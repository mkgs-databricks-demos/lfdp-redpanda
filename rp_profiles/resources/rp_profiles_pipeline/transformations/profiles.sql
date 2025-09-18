-- Create and populate the target table.

CREATE OR REFRESH STREAMING TABLE profiles
(
  user_id STRING PRIMARY KEY COMMENT 'Unique identifier for each user',
  email STRING COMMENT 'User email address',
  first_name STRING COMMENT 'User first name',
  last_name STRING COMMENT 'User last name',
  last_login TIMESTAMP_LTZ COMMENT 'Timestamp of the last login',
  registration_date DATE COMMENT 'Date of user registration',
  preferences STRUCT<language: STRING, notifications: STRING> COMMENT 'User preferences including language and notification settings',
  subscription_level STRING COMMENT 'Level of user subscription',
  source STRUCT<
    topic: STRING,
    partition: INT,
    offset: BIGINT,
    timestamp: TIMESTAMP_LTZ,
    timestampType: INT,
    ingestTime: TIMESTAMP_LTZ
  > COMMENT 'Information about the source of the data including the kafka topic, partition, offset, timestamp, timestamp type and ingestion time in Databricks.',
  CONSTRAINT valid_language EXPECT (preferences.language = 'en') ON VIOLATION DROP ROW,
  CONSTRAINT preferred_email_non_null EXPECT (
    case 
      when preferences.notifications = 'email' then (email is not null)
      else true 
    end
  )
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'quality' = 'silver'
);


CREATE STREAMING TABLE profiles_quarantine(
  CONSTRAINT english EXPECT (preferences.language <> 'en') ON VIOLATION DROP ROW
) AS SELECT *
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
    ,named_struct(
        'topic', topic,
        'partition', partition,
        'offset', offset,
        'timestamp', timestamp,
        'timestampType', timestampType,
        'ingestTime', ingestTime
     ) as source
    ,variant_col:email::string as email
    ,variant_col:first_name::string as first_name
    ,variant_col:last_name::string as last_name
    ,variant_col:last_login::timestamp as last_login
    ,variant_col:registration_date::date as registration_date
    ,variant_col:preferences::struct<language: string, notifications: string> as preferences
    ,variant_col:subscription_level::string as subscription_level
    ,variant_col:user_id::string as user_id
    ,_change_type as profiles_cdf_change_type
    ,_commit_version as profiles_cdf_commit_version
    ,_commit_timestamp as profiles_cdf_commit_timestamp
);


CREATE FLOW profiles_cdc AS AUTO CDC INTO
  profiles
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
    ,named_struct(
        'topic', topic,
        'partition', partition,
        'offset', offset,
        'timestamp', timestamp,
        'timestampType', timestampType,
        'ingestTime', ingestTime
     ) as source
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
APPLY AS TRUNCATE WHEN
  _change_type = "truncate"
SEQUENCE BY
  (timestamp, _commit_timestamp)
COLUMNS * EXCEPT
  (_change_type, _commit_version, _commit_timestamp, topic, partition, offset, timestamp, timestampType, ingestTime)
STORED AS
  SCD TYPE 1;