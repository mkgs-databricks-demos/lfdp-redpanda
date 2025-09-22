from pyspark import pipelines as dp

sink_catalog = spark.conf.get("sink_catalog")
sink_schema = spark.conf.get("sink_schema")

topics = ["profiles"]

for topic in topics:
    @dp.view(
        name = f"v_{topic}_sink",
    )
    def profiles_cdf():
    df = spark.read.table(f"{sink_catalog}.{sink_schema}.{topic}_sink" )
    return df