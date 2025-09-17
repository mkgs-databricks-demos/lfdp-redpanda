import dlt

@dlt.view()
def profiles_cdf():
  df = spark.readStream.option("readChangeFeed", "true").table(f"profiles_bronze")
  return df