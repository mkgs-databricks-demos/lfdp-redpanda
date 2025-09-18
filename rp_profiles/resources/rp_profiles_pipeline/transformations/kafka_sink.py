from utilities import utils
import os
import json
sink_definitions = "../config"
json_files = [f for f in os.listdir(data_sources_dir) if f.endswith('sink.json')]

table_definitions = []
for json_file in json_files:
    with open(os.path.join(data_sources_dir, json_file), 'r') as file:
        table_definitions.append(json.load(file))




redpanda_config = utils.get_redpanda_config(spark = spark, dbutils = dbutils)

# spark config 
catalog_use = spark.conf.get("catalog_use")
schema_use = spark.conf.get("schema_use")

# single topic 
topics = ["profiles"]
# uncomment to see how to ingest multiple topics to bronze tables
# topics = ["profiles", "hello-world", "__redpanda.connect.status", "__redpanda.connect.logs"]

for table_definition in table_definitions:
    kakfa_sink = utils.Sink(
        spark = spark
        ,topic = table_definition.get("topic")
        ,table_name = table_defintion.get("table_name")
        ,redpanda_config = redpanda_config
    )
    kakfa_sink.topic_sink()
