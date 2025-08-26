import dlt

@dlt.table(
  temporary=True
)
def profiles_cdf():
  df = spark.readStream.option("readChangeFeed", "true").option('startingVersion', 4).table(f"profiles_bronze")
  return df