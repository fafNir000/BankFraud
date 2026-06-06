import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import time

# --- НАСТРОЙКА СТРАНИЦЫ ---
st.set_page_config(
    page_title="PaySim Anti-Fraud Dashboard",
    page_icon="🛡️",
    layout="wide"
)

# --- ПОДКЛЮЧЕНИЕ К POSTGRESQL (ОСНОВАНО НА DOCKER COMPOSE) ---
def get_data():
    conn = psycopg2.connect(
        host="postgres",       # Имя сервиса из твоего docker-compose.yml
        database="fraud_db",    # POSTGRES_DB
        user="postgres",        # POSTGRES_USER
        password="secret_password",  # POSTGRES_PASSWORD
        port="5432"
    )
    # Забираем последние 5000 записей для аналитики
    query = """
        SELECT step, type, amount, oldbalanceOrg, newbalanceOrig, 
               timestamp, transaction_id, is_fraud_pred, fraud_probability 
        FROM transactions 
        ORDER BY transaction_id DESC 
        LIMIT 5000;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# --- ЗАГОЛОВОК ДАШБОРДА ---
st.title("🛡️ Мониторинг банковских транзакций Anti-Fraud в реальном времени")
st.write("Дашборд запущен внутри Docker-контейнера и запрашивает данные из PostgreSQL напрямую.")

# Контейнер для динамического обновления контента без перезагрузки всей страницы
placeholder = st.empty()

# --- ЦИКЛ ОБНОВЛЕНИЯ ДАННЫХ (СТРИМИНГ) ---
while True:
    try:
        df = get_data()
        
        with placeholder.container():
            if df.empty:
                st.warning("⏳ База данных пока пуста. Ожидаем батчи от PySpark...")
            else:
                # 1. Расчет ключевых метрик (KPI)
                total_tx = len(df)
                fraud_df = df[df['is_fraud_pred'] == 1]
                total_fraud = len(fraud_df)
                fraud_percentage = (total_fraud / total_tx) * 100 if total_tx > 0 else 0
                total_fraud_volume = fraud_df['amount'].sum()

                # Отображение метрик в один ряд
                kpi1, kpi2, kpi3, kpi4 = st.columns(4)
                kpi1.metric(label="Всего транзакций (в кэше)", value=f"{total_tx:,}")
                kpi2.metric(label="🚨 Выявлено Фрода", value=f"{total_fraud:,}", delta=f"{fraud_percentage:.2f}% от общего числа", delta_color="inverse")
                kpi3.metric(label="💰 Объем мошенничества", value=f"${total_fraud_volume:,.2f}")
                kpi4.metric(label="⚡ Статус контейнера", value="ONLINE", delta="Стрим идет")

                st.markdown("---")

                # 2. Визуализация (Графики)
                col1, col2 = st.columns(2)

                with col1:
                    st.subheader("📊 Распределение транзакций по типам")
                    type_counts = df['type'].value_counts().reset_index()
                    type_counts.columns = ['Тип', 'Количество']
                    fig_pie = px.pie(type_counts, values='Количество', names='Тип', 
                                     hole=0.4, color_discrete_sequence=px.colors.sequential.RdBu)
                    st.plotly_chart(fig_pie, use_container_width=True)

                with col2:
                    st.subheader("📈 Скоринг транзакций моделью XGBoost")
                    # Отображаем последние 500 транзакций для сохранения плавности интерфейса
                    fig_scatter = px.scatter(
                        df.head(500), 
                        x='transaction_id', 
                        y='fraud_probability', 
                        color='is_fraud_pred',
                        color_continuous_scale=['#2ca02c', '#d62728'],
                        labels={'fraud_probability': 'Вероятность фрода', 'transaction_id': 'ID Транзакции'},
                        title="Последние 500 транзакций и их вероятность мошенничества"
                    )
                    fig_scatter.update_layout(coloraxis_showscale=False)
                    st.plotly_chart(fig_scatter, use_container_width=True)

                # 3. Сводная таблица с фрод-алёртами
                st.subheader("🚨 Свежие фрод-алёрты (Подозрительные операции)")
                if not fraud_df.empty:
                    st.dataframe(
                        fraud_df[['transaction_id', 'type', 'amount', 'oldbalanceOrg', 'fraud_probability']]
                        .head(10)
                        .style.background_gradient(subset=['fraud_probability'], cmap='Reds'),
                        use_container_width=True
                    )
                else:
                    st.success("Чисто! Мошеннических транзакций в текущем батче не обнаружено.")

    except Exception as e:
        st.error(f"Ошибка подключения к базе данных или ожидания инициализации структуры: {e}")
        
    # Частота обновления экрана — каждые 3 секунды
    time.sleep(3)