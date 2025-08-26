CREATE STREAMING TABLE profiles 
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.variantType-preview' = 'supported',
  'quality' = 'bronze'
)
AS 
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
FROM (
  FROM STREAM(profiles_bronze)
  SELECT *, parse_json(value_str) as variant_col
);