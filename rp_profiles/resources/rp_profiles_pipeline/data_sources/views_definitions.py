from pyspark import pipelines as dp

sink_catalog = spark.conf.get("sink_catalog")
sink_schema = spark.conf.get("sink_schema")

topics = ["profiles"]

for topic in topics:
    @dp.view(
        name = f"v_{topic}_sink"
    )
    def read_sink():
        return (
            spark.read
            .option('skipChangeCommits','true')
            .table(f"{sink_catalog}.{sink_schema}.{topic}_sink")
            .orderBy("ingestTime")
            .dropDuplicates(["recordId"])
        )
    
    @dp.view(
        name = f"{topic}_cdf"
    )
    def read_cdf():
        df = spark.readStream.option("readChangeFeed", "true").table(f"{topic}_bronze")
        return df