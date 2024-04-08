from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql import DataFrame
import logging


# Initialize logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


# Initialize Spark Session
spark = SparkSession.builder \\
    .appName("Ecommerce Data Analysis") \\
    .config("spark.es.nodes", "elasticsearch") \\
    .config("spark.es.port", "9200") \\
    .config("spark.es.nodes.wan.only", "true") \\
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

# Kafka configuration
kafka_bootstrap_servers = "broker:29092,broker2:29094"

