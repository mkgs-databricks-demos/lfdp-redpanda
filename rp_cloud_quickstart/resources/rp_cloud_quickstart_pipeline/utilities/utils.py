from pyspark.sql.functions import udf
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
