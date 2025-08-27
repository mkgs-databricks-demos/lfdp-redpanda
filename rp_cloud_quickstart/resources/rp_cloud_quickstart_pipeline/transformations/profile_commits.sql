CREATE OR REFRESH STREAMING TABLE profile_commits AS 
SELECT 
  _change_type as change_type
  ,_commit_version as commit_version
  ,_commit_timestamp as commit_timestamp
  ,count(*) as rcrd_cnt
FROM 
  STREAM(profiles_cdf)
GROUP BY ALL
;
