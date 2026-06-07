import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    udf
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    LongType
)

from pyspark.ml import PipelineModel

# ==================================================
# ENV
# ==================================================

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# ==================================================
# SPARK SESSION
# ==================================================

spark = SparkSession.builder \
    .appName("FraudDetectionStreaming") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            "org.postgresql:postgresql:42.6.0"
        ])
    ) \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Started")

# ==================================================
# LOAD TRAINED MODEL
# ==================================================

MODEL_PATH = "models/paysim_logreg_model"

model = PipelineModel.load(MODEL_PATH)

print("✅ Model Loaded")

# ==================================================
# KAFKA SCHEMA
# ==================================================

schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("timestamp", LongType(), True),
    StructField("transaction_id", StringType(), True)
])

# ==================================================
# READ STREAM FROM KAFKA
# ==================================================

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# ==================================================
# JSON -> DATAFRAME
# ==================================================

transactions_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(
        from_json(
            col("json_str"),
            schema
        ).alias("data")
    ) \
    .select("data.*")

# ==================================================
# FEATURE ENGINEERING
# ==================================================

features_df = transactions_df \
    .withColumn(
        "errorBalanceOrig",
        col("newbalanceOrig")
        + col("amount")
        - col("oldbalanceOrg")
    ) \
    .withColumn(
        "errorBalanceDest",
        col("oldbalanceDest")
        + col("amount")
        - col("newbalanceDest")
    )

# ==================================================
# PREDICTION
# ==================================================

predictions_df = model.transform(features_df)

# ==================================================
# PROBABILITY
# ==================================================

probability_udf = udf(
    lambda v: float(v[1]),
    DoubleType()
)

predictions_df = predictions_df.withColumn(
    "fraud_probability",
    probability_udf(col("probability"))
)

# ==================================================
# FINAL DATAFRAME
# ==================================================

final_df = predictions_df.select(
    col("transaction_id"),
    col("timestamp"),
    col("step"),
    col("type"),
    col("amount"),
    col("prediction"),
    col("fraud_probability")
)

# ==================================================
# POSTGRES WRITER
# ==================================================

POSTGRES_URL = "jdbc:postgresql://localhost:5432/fraud_db"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "secret_password"

def write_to_postgres(batch_df, batch_id):

    count = batch_df.count()

    print(
        f"📦 Batch {batch_id} | Records = {count}"
    )

    if count == 0:
        return

    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "transactions") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# ==================================================
# START STREAM
# ==================================================

query = final_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

print("🚀 Streaming Started")

query.awaitTermination()