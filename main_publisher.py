from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count
from pyspark.sql.types import StructType, StringType
from kafka import KafkaProducer
from tabulate import tabulate
import math
import json

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("EmojiProcessor") \
    .master("local[*]") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.memory.fraction", "0.9") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "emoji-topic"
OUTPUT_TOPIC = "main-pub-topic"
CONSUMER_GROUP = "emoji-stream"
COMPRESSION = 1000

# Define Schema for Parsing
schema = StructType() \
    .add("user_id", StringType()) \
    .add("emoji", StringType()) \
    .add("timestamp", StringType())

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read Stream from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("kafka.group.id", CONSUMER_GROUP) \
    .option("maxOffsetsPerTrigger", 2000) \
    .load()

# Parse Kafka Messages
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Process Micro-Batches
def process_batch(df, batch_id):
    if not df.rdd.isEmpty():
        # Aggregate Emoji Counts
        emoji_counts = df.groupBy("emoji").agg(count("*").alias("count"))
        results = emoji_counts.collect()
        
        if results:
            # Log Results Locally
            headers = ["Emoji", "Count"]
            table = tabulate(results, headers=headers, tablefmt="grid")
            print(f"Processing batch {batch_id}")
            print(table)
            
            # Send Aggregated Data to Kafka
            for row in results:
                data = {"emoji": row["emoji"], "count": math.ceil(row["count"] / COMPRESSION)}
                print(f"Sending to main-pub-topic: {data}")
                producer.send(OUTPUT_TOPIC, value=data)
            producer.flush()

# Write Stream with Processing
query = parsed_stream.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .trigger(processingTime="2 seconds") \
    .start()

query.awaitTermination()
