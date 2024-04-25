from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql import DataFrame
import logging


# Initialize logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FakeEcommerce Data Analysis") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

# Kafka configuration
kafka_bootstrap_servers = "broker:29092,broker2:29094"

# Read data from 'ecommerce_customers' topic
customerSchema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("location", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("account_created", StringType(), True),
    StructField("last_login", TimestampType(), True),
])

customerDF = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json("value", customerSchema).alias("data"))
                .select("data.*")
                .withWatermark("last_login", "2 hours")
                )