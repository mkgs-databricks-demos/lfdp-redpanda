CREATE OR REFRESH MATERIALIZED VIEW mv_random_offset_deletes AS
WITH minmax AS (
  SELECT 
    MIN(offset) AS min_offset,
    MAX(offset) AS max_offset
  FROM profiles
)
SELECT 
  CASE 
    when FLOOR(minmax.min_offset + (minmax.max_offset - minmax.min_offset) * random()) >= minmax.max_offset THEN minmax.max_offset 
    else FLOOR(minmax.min_offset + (minmax.max_offset - minmax.min_offset) * random()) 
  END AS random_offsets
FROM minmax
CROSS JOIN (SELECT explode(sequence(0, 9)) AS n) t2
;