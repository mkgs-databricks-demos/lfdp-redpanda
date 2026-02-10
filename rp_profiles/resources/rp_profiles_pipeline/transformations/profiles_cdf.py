from pyspark import pipelines as dp

@dp.temporary_view()
def profiles_cdf():
  df = spark.readStream.option("readChangeFeed", "true").table(f"profiles_bronze")
  return df