# 🛡️ Real-Time Banking Fraud Detection System
Contributors:  

This project demonstrates a full real-time fraud detection pipeline:

Kafka → Spark → ML → PostgreSQL → Dashboard

Built for learning Big Data, streaming systems, and production-style ML pipelines.

End-to-end **real-time streaming system** for detecting fraudulent banking transactions using:

- Kafka (data streaming)
- Spark Structured Streaming (processing)
- Machine Learning (Logistic Regression)
- PostgreSQL (storage)
- Streamlit (dashboard)
- Docker Compose (orchestration)

---

# ⚙️ Architecture


CSV Dataset
↓
Kafka Producer (generator.py)
↓
Kafka Topic: transactions
↓
Spark Streaming Processor (spark_processor.py)
↓
ML Model (Logistic Regression)
↓
PostgreSQL Database
↓
Streamlit Dashboard


---

# 🚀 Features

- Real-time transaction streaming
- Fraud detection using ML model
- Feature engineering in Spark
- Kafka-based event pipeline
- PostgreSQL storage layer
- Interactive dashboard with charts
- Dockerized infrastructure

---

# 🧱 Tech Stack

- Python 3.11
- Apache Kafka
- Apache Spark (Structured Streaming + MLlib)
- PostgreSQL 15
- Streamlit
- Docker & Docker Compose
- Pandas / Scikit-learn / PySpark

---

# 📁 Project Structure

.
├── data/
│ ├── data.csv
│ └── modeldata.csv
│
├── models/
│ └── paysim_logreg_model/
│
├── scripts/
│ ├── generator.py
│ ├── train_model.py
│ └── spark_processor.py
│
├── dashboard/
│ ├── dashboard.py
│ └── requirements_dash.txt
│
├── cleaning.ipynb
├── docker-compose.yml
├── Dockerfile.dashboard
└── README.md

---

# 🐳 Running with Docker

## 1. Start all services

bash
docker-compose up --build

This will start:

Zookeeper
Kafka
PostgreSQL
Data Generator
Streamlit Dashboard

---


##2. Train ML Model

Before starting Spark streaming:

python scripts/train_model.py

Model will be saved to:

models/paysim_logreg_model

##3. Start Spark Streaming Processor

python scripts/spark_processor.py


---

##4. Open Dashboard

http://localhost:8501

##Dataset
Input dataset:
data/data.csv
After preprocessing:
data/modeldata.csv

Columns include:

step
type
amount
oldbalanceOrg
newbalanceOrig
oldbalanceDest
newbalanceDest
isFraud (label)

---

##Generator (Kafka Producer)
scripts/generator.py

Reads CSV data
Removes label columns
Adds:
transaction_id
timestamp
Sends data to Kafka topic transactions

##Spark Streaming Pipeline

scripts/spark_processor.py
Steps:
Read data from Kafka
Parse JSON into DataFrame
Feature engineering:
errorBalanceOrig
errorBalanceDest
Load trained ML model
Predict fraud probability
Write results into PostgreSQL

##Machine Learning Model

scripts/train_model.py
Algorithm:
Logistic Regression
Pipeline:
StringIndexer (type)
OneHotEncoder
VectorAssembler
Output:
models/paysim_logreg_model

PostgreSQL Schema
Table:
transactions

Columns:

transaction_id
timestamp
step
type
amount
prediction
fraud_probability

Streamlit Dashboard

dashboard/dashboard.py

Features:
Real-time KPIs
Fraud detection metrics
Transaction type distribution
Fraud probability visualization
Top fraud alerts
Raw transaction view

Docker Services
Defined in docker-compose.yml:

kafka
zookeeper
postgres
generator
dashboard
