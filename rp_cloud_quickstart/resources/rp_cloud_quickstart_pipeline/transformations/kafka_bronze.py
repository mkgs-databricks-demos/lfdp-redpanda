# import dlt
# from pyspark.sql.functions import col, current_timestamp
from utilities import utils

redpanda_config = utils.get_redpanda_config(spark = spark, dbutils = dbutils)

# spark config 
catalog_use = spark.conf.get("catalog_use")
schema_use = spark.conf.get("schema_use")
event_log = spark.conf.get("event_log")
sink_catalog = spark.conf.get("sink_catalog")
sink_schema = spark.conf.get("sink_schema")

# topics = ["profiles"]

# uncomment to see how to ingest multiple topics to bronze tables
topics = ["profiles", "hello-world", "__redpanda.connect.status", "__redpanda.connect.logs"]

for topic in topics:
    kakfa_ingest = utils.Bronze(
        spark = spark
        ,topic = topic
        ,catalog = catalog_use
        ,schema = schema_use
        ,event_log = event_log
        ,sink_catalog = sink_catalog
        ,sink_schema = sink_schema
        ,redpanda_config = redpanda_config
    )
    kakfa_ingest.topic_ingestion()
    kakfa_ingest.backfill_full_refresh()
