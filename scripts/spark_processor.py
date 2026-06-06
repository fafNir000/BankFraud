import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, split
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, IntegerType, LongType

os.environ["HADOOP_HOME"] = "C:\\hadoop"

# Указываем Спарку жесткий путь к Python из твоего виртуального окружения
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Получаем абсолютный путь к папке проекта
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 1. Инициализация сессии Spark под локальный режим master("local[*]")
spark = SparkSession.builder \
    .appName("PaySim-Fraud-Detector-Local") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Определение схемы входящего JSON из Kafka
schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("timestamp", LongType(), True),
    StructField("transaction_id", LongType(), True)
])

# Порядок колонок строго соответствует X_train.columns из твоего Jupyter Notebook
FEATURE_COLUMNS = [
    'step', 'amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest',
    'errorBalanceOrig', 'errorBalanceDest', 
    'type_CASH_IN', 'type_CASH_OUT', 'type_DEBIT', 'type_PAYMENT', 'type_TRANSFER'
]

# Глобальные переменные для кэширования моделей на воркерах
_model = None
_scaler = None
_threshold = None

# 3. Оптимизированная функция предсказания (UDF)
def predict_fraud_udf(step, t_type, amount, old_org, new_orig, old_dest, new_dest):
    global _model, _scaler, _threshold
    
    # Защита от пустых значений (если прилетит битый JSON)
    if any(v is None for v in [step, t_type, amount, old_org, new_orig, old_dest, new_dest]):
        return "0,0.0000"
        
    # Ленивая инициализация: загружаем артефакты ровно ОДИН раз на процесс воркера
    if _model is None:
        import joblib
        import xgboost
        
        # Строим железные абсолютные пути к файлам моделей на Windows
        MODEL_PATH = os.path.join(BASE_DIR, "models", "fraud_model_xgb.pkl")
        SCALER_PATH = os.path.join(BASE_DIR, "models", "scaler.pkl")
        THRESHOLD_PATH = os.path.join(BASE_DIR, "models", "threshold.pkl")
        
        _model = joblib.load(MODEL_PATH)
        _scaler = joblib.load(SCALER_PATH)
        _threshold = joblib.load(THRESHOLD_PATH)
    
    # Feature Engineering (Расчет аномалий баланса)
    errorBalanceOrig = float(new_orig) + float(amount) - float(old_org)
    errorBalanceDest = float(old_dest) + float(amount) - float(new_dest)
    
    # Эмуляция One-Hot Encoding (pd.get_dummies)
    types = ['CASH_IN', 'CASH_OUT', 'DEBIT', 'PAYMENT', 'TRANSFER']
    type_dummies = {f'type_{t}': 1.0 if t_type == t else 0.0 for t in types}
    
    # Формируем словарь признаков
    row_features = {
        'step': float(step),
        'amount': float(amount),
        'oldbalanceOrg': float(old_org),
        'newbalanceOrig': float(new_orig),
        'oldbalanceDest': float(old_dest),
        'newbalanceDest': float(new_dest),
        'errorBalanceOrig': errorBalanceOrig,
        'errorBalanceDest': errorBalanceDest,
        **type_dummies
    }
    
    # Собираем строго упорядоченный вектор для скейлера
    features_vector = [row_features[col_name] for col_name in FEATURE_COLUMNS]
    
    # Масштабирование и предсказание XGBoost
    features_scaled = _scaler.transform([features_vector])
    proba = _model.predict_proba(features_scaled)[:, 1][0]
    
    # Определение класса по кастомному порогу
    is_fraud = 1 if proba >= _threshold else 0
    return f"{is_fraud},{proba:.4f}"

# Регистрируем UDF в контексте Спарка
predict_fraud = udf(predict_fraud_udf, StringType())

# 4. Чтение потока из Kafka (Смотрим на localhost:9092, так как Спарк запущен локально)
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Десериализация строки JSON из Кафки
transactions_df = kafka_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# 5. Применение ML-модели к потоку данных
processed_df = transactions_df.withColumn(
    "prediction_raw", 
    predict_fraud(
        col("step"), col("type"), col("amount"), 
        col("oldbalanceOrg"), col("newbalanceOrig"), 
        col("oldbalanceDest"), col("newbalanceDest")
    )
)

# Разделяем текстовый результат на две колонки
final_df = processed_df \
    .withColumn("is_fraud_pred", split(col("prediction_raw"), ",").getItem(0).cast(IntegerType())) \
    .withColumn("fraud_probability", split(col("prediction_raw"), ",").getItem(1).cast(DoubleType())) \
    .drop("prediction_raw")

# 6. Функция записи микро-батчей в PostgreSQL (Стучимся на localhost:5432)
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() > 0:
        print(f"📥 Обработан батч #{batch_id}. Запись {batch_df.count()} транзакций в PostgreSQL...")
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/fraud_db") \
            .option("dbtable", "transactions") \
            .option("user", "postgres") \
            .option("password", "secret_password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

# 7. Запуск стриминга
query = final_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .start()

print("🚀 Локальный PySpark процессор успешно запущен и слушает Docker-Kafka на порту 9092...")
query.awaitTermination()
