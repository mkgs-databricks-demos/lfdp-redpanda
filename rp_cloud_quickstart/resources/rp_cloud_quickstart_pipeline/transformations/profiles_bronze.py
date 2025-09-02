import dlt
from pyspark.sql.functions import col, current_timestamp
from utilities import utils

redpanda_config = utils.get_redpanda_config(spark = spark, dbutils = dbutils)

topics = ["profiles"]

# uncomment to see how to ingest multiple topics to bronze tables
# topics = ["profiles", "hello-world", "__redpanda.connect.status", "__redpanda.connect.logs"]

class Bronze:
    def __init__(self, topic):
        self.topic = topic

    def topic_ingestion(self):

        topic_name = self.topic.replace('-', '_').replace(".", "_")

        @dlt.table(
            name = f"{topic_name}_bronze"
            ,table_properties={
                'quality' : 'bronze'
                ,'delta.enableChangeDataFeed' : 'true'
                ,'delta.enableDeletionVectors' : 'true'
                ,'delta.enableRowTracking' : 'true'
            }
        )
        def profiles_bronze():
            """
            Read Stream from the Redpanda Enterprise Quickstart "logins" topic. 
            """
            return (
                spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers",redpanda_config.get("bootstrap.servers"))
                .option("subscribe", self.topic)
                .option("kafka.sasl.mechanism", redpanda_config.get("sasl.mechanism"))
                .option("kafka.security.protocol", redpanda_config.get("security.protocol"))
                .option("kafka.sasl.jaas.config", redpanda_config.get("sasl.jaas.config"))
                # Optional: Set failOnDataLoss to false to avoid query failure if data is missing
                .option("failOnDataLoss", "false")
                # Optional: Set startingOffsets to earliest for initial testing
                .option("startingOffsets", "earliest")
                .load()
                .withColumn("value_str", col("value").cast("string"))
                .withColumn("ingestTime", current_timestamp())
            )

for topic in topics:
    kakfa_ingest = Bronze(topic)
    kakfa_ingest.topic_ingestion()
