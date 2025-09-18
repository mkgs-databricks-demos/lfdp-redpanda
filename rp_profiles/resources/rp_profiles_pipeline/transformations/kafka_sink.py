from utilities import utils
import os
import json

redpanda_config = utils.get_redpanda_config(spark = spark, dbutils = dbutils)

sink_definitions = "../config"
json_files = [f for f in os.listdir(data_sources_dir) if f.endswith('sink.json')]

table_definitions = []
for json_file in json_files:
    with open(os.path.join(data_sources_dir, json_file), 'r') as file:
        table_definitions.append(json.load(file))

for table_definition in table_definitions:
    kakfa_sink = utils.Sink(
        spark = spark
        ,topic = table_definition.get("topic")
        ,table_name = table_defintion.get("table_name")
        ,key_columns = table_definition.get("key_columns")
        ,value_columns = table_definition.get("value_columns")
        ,transformation = table_definition.get("transformation", None)
        ,redpanda_config = redpanda_config
    )
    kakfa_sink.table_sink_to_kafka()
