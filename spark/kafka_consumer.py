from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging

logger = logging.getLogger("org")
logger.setLevel(logging.FATAL)

#.config("spark.local.‌​dir","D:/spark_temp") \
spark = SparkSession.builder.appName("KafkaStructuredStreaming").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.local.‌​dir", "D:/spark_temp").getOrCreate()

kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'weatherkafkatopic'

json_schema = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("temp_c", DoubleType(), True),
    StructField("wind_mph", DoubleType(), True),
    StructField("precip_mm", DoubleType(), True),
    StructField("condition", StringType(), True),
])

raw_data_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()


raw_data_stream = raw_data_stream.selectExpr("CAST(value AS STRING)")

structured_data_stream = raw_data_stream.select(
    from_json(col("value"), json_schema).alias("data")
).select("data.*")

delta_path = "/home/hoan123/project-master/delta_lake/delta_lake_table_4"  

query = structured_data_stream.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "/home/hoan123/project-master/delta_lake/checkpoint_4") \
    .start(delta_path)


query.awaitTermination()

# spark.stop()