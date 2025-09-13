import dlt
from pyspark.sql.functions import col, current_timestamp, lit, udf
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from typing import Any


def get_redpanda_config(spark: SparkSession, dbutils: Any) -> dict:
    """
    Return a dictionary of Redpanda configuration options. 
    Note that a Databricks secret scope with keys for the bootstrap server, username, and password must be created prior to running this utility.
    Additionally please note that the ScramLoginModule used for the sasl.jaas.config is specific to Databricks Serverless Compute only.  
    """

    secret_scope = spark.conf.get("secret_scope")
    secret_key_user = spark.conf.get("secret_key_user")
    secret_key_password = spark.conf.get("secret_key_password")
    secret_key_bootstrap_server = spark.conf.get("secret_key_bootstrap_server")

    return {
        "bootstrap.servers": dbutils.secrets.get(scope=secret_scope, key=secret_key_bootstrap_server),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{dbutils.secrets.get(scope=secret_scope, key=secret_key_user)}' password='{dbutils.secrets.get(scope=secret_scope, key=secret_key_password)}';"
    }

class Bronze:
    def __init__(self, spark: SparkSession, topic: str, catalog: str, schema: str, sink_catalog: str, sink_schema: str, event_log: str, redpanda_config: dict):
        self.spark = spark
        self.topic = topic
        self.catalog = catalog
        self.schema = schema
        self.event_log = event_log
        self.sink_catalog = sink_catalog
        self.sink_schema = sink_schema
        self.redpanda_config = redpanda_config
        self.topic_name = self.topic.replace('-', '_').replace(".", "_")

    def topic_ingestion(self):

        @dlt.table(
            name = f"{self.topic_name}_bronze"
            ,table_properties={
                'quality' : 'bronze'
                ,'delta.enableChangeDataFeed' : 'true'
                ,'delta.enableDeletionVectors' : 'true'
                ,'delta.enableRowTracking' : 'true'
                ,'delta.autoOptimize.optimizeWrite': 'true'
                ,'delta.autoOptimize.autoCompact': 'true'
            }
        )
        def kafka_bronze():
            """
            Read Stream from the Redpanda Enterprise Quickstart "logins" topic. 
            """
            return (
                self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers",self.redpanda_config.get("bootstrap.servers"))
                .option("subscribe", self.topic)
                .option("kafka.sasl.mechanism", self.redpanda_config.get("sasl.mechanism"))
                .option("kafka.security.protocol", self.redpanda_config.get("security.protocol"))
                .option("kafka.sasl.jaas.config", self.redpanda_config.get("sasl.jaas.config"))
                # Optional: Set failOnDataLoss to false to avoid query failure if data is missing
                .option("failOnDataLoss", "false")
                # Optional: Set startingOffsets to earliest for initial testing
                .option("startingOffsets", "earliest")
                .load()
                .withColumn("value_str", col("value").cast("string"))
                .withColumn("ingestTime", current_timestamp())
            )

        dlt.create_sink(
            name = f"{self.topic_name}_bronze_delta_sink" 
            ,format = "delta"
            ,options={
                "tableName": f"{self.sink_catalog}.{self.sink_schema}.{self.topic_name}_kafka_delta_sink",
                "quality": "bronze",
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true"
            }
        )

        @dlt.append_flow(
            name = f"flow_{self.topic_name}_bronze_delta_sink"
            ,target = f"{self.topic_name}_bronze_delta_sink"
        )
        def delta_sink_flow():
            return self.spark.readStream.table(f"{self.topic_name}_bronze") 
        
    def backfill_full_refresh(self):
        @dlt.append_flow(
            target = f"{self.topic_name}_bronze",
            once = True,
            name = f"flow_refresh_{self.topic_name}_bronze",
            comment = f"Backfill data no longer available in kafka in bronze"
        )
        def backfill():
            return (
                self.spark.read.table(f"{self.sink_catalog}.{self.sink_schema}.{self.topic_name}_kafka_delta_sink")
            )
