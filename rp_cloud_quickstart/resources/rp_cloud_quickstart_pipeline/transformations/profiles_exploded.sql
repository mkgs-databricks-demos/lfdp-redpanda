CREATE STREAMING TABLE profiles_exploded 
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.variantType-preview' = 'supported',
  'quality' = 'bronze'
)
AS 
SELECT 
  *
  ,parse_json(value_str) as variant_col
FROM STREAM(profiles);