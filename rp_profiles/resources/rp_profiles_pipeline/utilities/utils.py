from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType
    ,StructField
    ,StringType
    ,BinaryType
    ,IntegerType
    ,LongType
    ,TimestampType
    ,FloatType
)
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import (
    col
    ,current_timestamp
    ,lit
    ,udf
    ,sha2
    ,concat_ws
)
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
    def __init__(self, spark: SparkSession, topic: str, catalog: str, schema: str, sink_catalog: str, sink_schema: str, event_log: str, redpanda_config: dict, startingOffsets: str = "latest"):
        self.spark = spark
        self.topic = topic
        self.catalog = catalog 
        self.schema = schema
        self.event_log = event_log
        self.sink_catalog = sink_catalog
        self.sink_schema = sink_schema
        self.redpanda_config = redpanda_config
        self.startingOffsets = startingOffsets
        self.topic_name = self.topic.replace('-', '_').replace(".", "_")

    @staticmethod
    def stream_kafka(self, starting_offsets: str):
        df = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers",self.redpanda_config.get("bootstrap.servers"))
            .option("subscribe", self.topic)
            .option("kafka.sasl.mechanism", self.redpanda_config.get("sasl.mechanism"))
            .option("kafka.security.protocol", self.redpanda_config.get("security.protocol"))
            .option("kafka.sasl.jaas.config", self.redpanda_config.get("sasl.jaas.config"))
            .option("failOnDataLoss", "false")
            .option("startingOffsets", starting_offsets)
            .load()
            
        )
        expr = f"sha2(concat_ws('||', {', '.join(df.columns)}), 256) as recordId"
        return df.selectExpr(expr, *df.columns)

    def topic_ingestion(self):

        # define target streaming table for bronze ingestion
        dp.create_streaming_table(
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

        # standard flow for ingesting data from kafka 
        @dp.append_flow(
            name = f"flow_{self.topic_name}_bronze"
            ,target = f"{self.topic_name}_bronze"
            ,comment = f"Incremental load of kafka data from {self.topic_name}."
        )
        def kafka_bronze():
            """
            Ingest data from Kafka into Bronze table
            """
            df = self.stream_kafka(self, starting_offsets = self.startingOffsets)
            return (
                df
                .withColumn("value_str", col("value").cast("string"))
                .withColumn("ingestTime", current_timestamp())
            )

        # full refresh backfill of bronze table from delta sink
        @dp.append_flow(
            name = f"flow_refresh_{self.topic_name}_bronze"
            ,target = f"{self.topic_name}_bronze"
            ,once = True
            ,comment = f"Backfill data no longer available in kafka in bronze from delta sink on full refresh."
        )
        def backfill(): 
            return (
                self.spark.read
                .table(f"v_{topic}_sink")
            )
            # except AnalysisException:
            #     df = (
            #         self.spark.range(0)
            #         .selectExpr(
            #             "CAST(NULL AS STRING) AS recordId",
            #             "CAST(NULL AS BINARY) AS key",
            #             "CAST(NULL AS BINARY) AS value",
            #             "CAST(NULL AS STRING) AS topic",
            #             "CAST(NULL AS INT) AS partition",
            #             "CAST(NULL AS BIGINT) AS offset",
            #             "CAST(NULL AS TIMESTAMP) AS timestamp",
            #             "CAST(NULL AS INT) AS timestampType",
            #             "CAST(NULL AS STRING) AS value_str",
            #             "CAST(NULL AS TIMESTAMP) AS ingestTime"
            #         )
            #     )
            # return df

    def sink_init(self):
        # create delta sink for backfill on full refresh
        dp.create_sink(
            name = f"{self.sink_catalog}.{self.sink_schema}.{self.topic_name}_sink" 
            ,format = "delta"
            ,options={
                "tableName": f"{self.sink_catalog}.{self.sink_schema}.{self.topic_name}_sink"
                ,"quality": "bronze"
                ,"delta.autoOptimize.optimizeWrite": "true"
                ,"delta.autoOptimize.autoCompact": "true"
                ,'delta.enableRowTracking' : 'true'
            }
        )

        # initial delta sink creation from source stream
        @dp.append_flow(
            name = f"flow_from_source_{self.topic_name}_to_sink"
            ,target = f"{self.sink_catalog}.{self.sink_schema}.{self.topic_name}_sink"
            ,once = True
        )
        def delta_sink_flow_from_source():
            df = self.stream_kafka(self, starting_offsets = "earliest")
            return (
                df
                .withColumn("value_str", col("value").cast("string"))
                .withColumn("ingestTime", current_timestamp())
            )

    def refresh_sink(self, from_bronze: bool = True):
        if from_bronze:
            # incremental update of delta sink from bronze table
            @dp.append_flow(
                name = f"flow_from_bronze_{self.topic_name}_to_sink"
                ,target = f"{self.sink_catalog}.{self.sink_schema}.{self.topic_name}_sink"
                ,comment = f"Incremental update of delta sink from bronze table."
            )
            def delta_sink_flow_from_bronze():
                return (
                    self.spark.readStream
                    .option('skipChangeCommits','true')
                    .table(f"{self.topic_name}_bronze")
                )
        else:
            @dp.append_flow(
                name = f"flow_refresed_from_source_{self.topic_name}_to_sink"
                ,target = f"{self.sink_catalog}.{self.sink_schema}.{self.topic_name}_sink"
            )
            def delta_sink_flow_from_source():
                df = self.stream_kafka(self, starting_offsets = "earliest")
                return (
                    df
                    .withColumn("value_str", col("value").cast("string"))
                    .withColumn("ingestTime", current_timestamp())
                )


class Sink:
    def __init__(self, spark: SparkSession, topic: str, table_name: str, key_columns: list, value_columns: list, redpanda_config: dict, transformations: list = None):
        self.spark = spark
        self.topic = topic
        self.table_name = table_name
        self.key_columns = key_columns
        self.value_columns = value_columns
        self.redpanda_config = redpanda_config
        self.transformations = transformations

    def table_sink_to_kafka(self):

        dp.create_sink(
            name = f"{self.topic}_sink"
            ,format = "kafka"
            ,options = {
                "kafka.bootstrap.servers": self.redpanda_config.get("bootstrap.servers")
                ,"topic": self.topic
                ,"kafka.sasl.mechanism": self.redpanda_config.get("sasl.mechanism")
                ,"kafka.security.protocol": self.redpanda_config.get("security.protocol")
                ,"kafka.sasl.jaas.config": self.redpanda_config.get("sasl.jaas.config")
            }
        )
       
        key_expr = ','.join(self.key_columns)
        value_expr = ','.join(self.value_columns)
        # transformations = ','.join(self.transformations)
        expr = f"to_json(struct({key_expr})) as key", f"to_json(struct({value_expr})) AS value"

        @dp.append_flow(name = "kafka_sink_flow", target = f"{self.topic}_sink")
        def kafka_sink_flow():
            df = self.spark.readStream.table(self.table_name)
            for transformation in self.transformations:
                df = df.selectExpr("*", transformation)
            df = df.selectExpr(*expr)
            return df


        
       
        


                     








