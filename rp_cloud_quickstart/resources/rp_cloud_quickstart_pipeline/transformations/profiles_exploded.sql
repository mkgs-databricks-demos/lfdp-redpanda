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
  ,profiles_exp.*
FROM (
  FROM STREAM(profiles)
  SELECT *, parse_json(value_str) as variant_col
)
,LATERAL variant_explode(variant_col) as profiles_exp;