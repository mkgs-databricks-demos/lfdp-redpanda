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

sink_catalog = spark.conf.get("sink_catalog")
sink_schema = spark.conf.get("sink_schema")

topics = ["profiles"]

for topic in topics:
    @dp.view(
        name = f"v_{topic}_sink"
    )
    def read_sink():
        try:
            df = (
                spark.read
                .option('skipChangeCommits','true')
                .table(f"{sink_catalog}.{sink_schema}.{topic}_sink")
                .orderBy("ingestTime")
                .dropDuplicates(["recordId"])
            )
        except AnalysisException:
            df = (
                spark.range(0)
                .selectExpr(
                    "CAST(NULL AS STRING) AS recordId",
                    "CAST(NULL AS BINARY) AS key",
                    "CAST(NULL AS BINARY) AS value",
                    "CAST(NULL AS STRING) AS topic",
                    "CAST(NULL AS INT) AS partition",
                    "CAST(NULL AS BIGINT) AS offset",
                    "CAST(NULL AS TIMESTAMP) AS timestamp",
                    "CAST(NULL AS INT) AS timestampType",
                    "CAST(NULL AS STRING) AS value_str",
                    "CAST(NULL AS TIMESTAMP) AS ingestTime"
                )
            )
        return df
    
    @dp.view(
        name = f"{topic}_cdf"
    )
    def read_cdf():
        df = spark.readStream.option("readChangeFeed", "true").table(f"{topic}_bronze")
        return df