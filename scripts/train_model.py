import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("PaySim-LogReg-Training") \
    .master("local[*]") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("data/modeldata.csv", header=True, inferSchema=True)

df = df.withColumn(
    "errorBalanceOrig",
    col("newbalanceOrig") + col("amount") - col("oldbalanceOrg")
).withColumn(
    "errorBalanceDest",
    col("oldbalanceDest") + col("amount") - col("newbalanceDest")
)

df = df.withColumn("label", col("isFraud").cast("double"))

indexer = StringIndexer(
    inputCol="type",
    outputCol="type_index",
    handleInvalid="keep"
)

encoder = OneHotEncoder(
    inputCol="type_index",
    outputCol="type_vec"
)

assembler = VectorAssembler(
    inputCols=[
        "step",
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
        "errorBalanceOrig",
        "errorBalanceDest",
        "type_vec"
    ],
    outputCol="features"
)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=20,
    regParam=0.1,
    elasticNetParam=0.8
)

pipeline = Pipeline(stages=[indexer, encoder, assembler, lr])

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

model = pipeline.fit(train_df)

predictions = model.transform(test_df)
predictions.select("label", "prediction", "probability").show(10)

MODEL_PATH = "models/paysim_logreg_model"
model.write().overwrite().save(MODEL_PATH)

print(f"Model saved to {MODEL_PATH}")