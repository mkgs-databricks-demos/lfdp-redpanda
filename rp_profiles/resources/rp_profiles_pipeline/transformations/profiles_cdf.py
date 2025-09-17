from pyspark import pipelines as dp

@dp.view()
def profiles_cdf():
  df = spark.readStream.option("readChangeFeed", "true").table(f"profiles_bronze")
  return df