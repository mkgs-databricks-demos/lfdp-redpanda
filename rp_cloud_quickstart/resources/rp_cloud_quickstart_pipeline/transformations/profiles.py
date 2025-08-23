import dlt
from pyspark.sql.functions import col
from utilities import utils

redpanda_config = utils.get_redpanda_config(spark = spark, dbutils = dbutils)

@dlt.table
def profiles():
    """
    Read Stream from the Redpanda Enterprise Quickstart "logins" topic. 
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",redpanda_config.get("bootstrap.servers"))
        .option("subscribe", "profiles")
        .option("kafka.sasl.mechanism", redpanda_config.get("sasl.mechanism"))
        .option("kafka.security.protocol", redpanda_config.get("security.protocol"))
        .option("kafka.sasl.jaas.config", redpanda_config.get("sasl.jaas.config"))
        # Optional: Set failOnDataLoss to false to avoid query failure if data is missing
        .option("failOnDataLoss", "false")
        # Optional: Set startingOffsets to earliest for initial testing
        .option("startingOffsets", "earliest")
        .load()
        .withColumn("value_str", col("value").cast("string"))
    )
