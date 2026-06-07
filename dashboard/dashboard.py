import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime

st.set_page_config(
    page_title="Anti-Fraud Dashboard",
    page_icon="🛡️",
    layout="wide"
)

@st.cache_data(ttl=5)
def load_data():

    conn = psycopg2.connect(
        host="postgres",
        database="fraud_db",
        user="postgres",
        password="secret_password",
        port="5432"
    )

    query = """
    SELECT
        transaction_id,
        timestamp,
        step,
        type,
        amount,
        prediction,
        fraud_probability
    FROM transactions
    ORDER BY transaction_id DESC
    LIMIT 5000
    """

    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

st.title("🛡️ Real-Time Bank Transacton Fraud Detection")

st.caption(
    "Kafka → Spark Streaming → Logistic Regression → PostgreSQL → Streamlit"
)

refresh = st.button("🔄 Refresh Data")

df = load_data()

if df.empty:
    st.warning("База данных пока пуста.")
    st.stop()

fraud_df = df[df["prediction"] == 1]

total_transactions = len(df)

total_fraud = len(fraud_df)

fraud_rate = (
    total_fraud / total_transactions * 100
    if total_transactions > 0 else 0
)

fraud_volume = fraud_df["amount"].sum()

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "All transactions",
    f"{total_transactions:,}"
)

col2.metric(
    "🚨 Fraud Detected",
    f"{total_fraud:,}"
)

col3.metric(
    "💰 Fraud Volume",
    f"${fraud_volume:,.2f}"
)

col4.metric(
    "⚡ Last Update",
    datetime.now().strftime("%H:%M:%S")
)

st.divider()

left, right = st.columns(2)

with left:

    st.subheader("📊 Transaction Types")

    type_counts = (
        df["type"]
        .value_counts()
        .reset_index()
    )

    type_counts.columns = [
        "Transaction Type",
        "Count"
    ]

    fig = px.pie(
        type_counts,
        values="Count",
        names="Transaction Type",
        hole=0.4
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


with right:

    st.subheader("📈 Fraud Probability")

    recent = (
        df.sort_values(
            "transaction_id",
            ascending=False
        )
        .head(500)
    )

    fig = px.scatter(
        recent,
        x="transaction_id",
        y="fraud_probability",
        color="prediction",
        hover_data=[
            "amount",
            "type"
        ]
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


st.subheader("🚨 Fraud Distribution by Type")

fraud_by_type = (
    fraud_df
    .groupby("type")
    .size()
    .reset_index(name="count")
)

if not fraud_by_type.empty:

    fig = px.bar(
        fraud_by_type,
        x="type",
        y="count"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

else:

    st.success(
        "Fraud transactions are not detected."
    )

st.subheader("🚨 Latest Fraud Alerts")

if not fraud_df.empty:

    alerts = fraud_df[
        [
            "transaction_id",
            "type",
            "amount",
            "fraud_probability"
        ]
    ].sort_values(
        "fraud_probability",
        ascending=False
    )

    st.dataframe(
        alerts.head(20),
        use_container_width=True
    )

else:

    st.success(
        "No suspicius transactions."
    )

with st.expander("📋 Show Raw Transactions"):

    st.dataframe(
        df.head(100),
        use_container_width=True
    )