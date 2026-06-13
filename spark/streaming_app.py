from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Créer la session Spark avec les bons packages
spark = SparkSession.builder \
    .appName("RealtimeWebLogs") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Schéma des logs JSON
schema = StructType() \
    .add("user", StringType()) \
    .add("page", StringType()) \
    .add("status", IntegerType()) \
    .add("timestamp", DoubleType())

# Lecture depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "web_logs") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast("timestamp"))

# 1️⃣ Nombre de visites par page
visits_by_page = df.groupBy("page").count()

# 2️⃣ Erreurs 404
errors_404 = df.filter(col("status") == 404).groupBy("page").count()

# 3️⃣ Utilisateurs actifs (fenêtre de 10 secondes)
active_users = df.groupBy(window("timestamp", "10 seconds"), "user").count()

# Sorties console
query1 = visits_by_page.writeStream \
    .outputMode("complete") \
    .format("console") \
    .queryName("visits_by_page") \
    .start()

query2 = errors_404.writeStream \
    .outputMode("complete") \
    .format("console") \
    .queryName("errors_404") \
    .start()

query3 = active_users.writeStream \
    .outputMode("complete") \
    .format("console") \
    .queryName("active_users") \
    .start()

# Attendre la terminaison
query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
