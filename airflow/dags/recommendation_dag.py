from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.redis.hooks.redis import RedisHook
from datetime import datetime, timedelta
import json
import os

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def read_aggregates_from_kafka(**context):
    """Упрощённое чтение последних сообщений из Kafka (можно заменить на Consumer)."""
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        'aggregated_clicks',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000
    )
    aggregates = {}
    for msg in consumer:
        pid = msg.value['product_id']
        cnt = msg.value['count']
        aggregates[pid] = aggregates.get(pid, 0) + cnt
    consumer.close()
    # Сохраняем в XCom
    context['ti'].xcom_push(key='aggregates', value=aggregates)

def join_with_orders(**context):
    """Читает агрегаты из XCom, запрашивает исторические заказы из BigQuery, объединяет."""
    aggregates = context['ti'].xcom_pull(key='aggregates', task_ids='read_kafka')
    if not aggregates:
        aggregates = {}

    # Получение данных из BigQuery
    hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT product_id, SUM(quantity) as total_orders, AVG(price) as avg_price
        FROM `your-project.raw.orders`
        GROUP BY product_id
    """)
    rows = cursor.fetchall()
    orders_data = {row[0]: {'total_orders': row[1], 'avg_price': row[2]} for row in rows}

    # Объединение
    final = []
    for pid, click_count in aggregates.items():
        orders = orders_data.get(pid, {'total_orders': 0, 'avg_price': 0})
        final.append({
            'product_id': pid,
            'click_count': click_count,
            'total_orders': orders['total_orders'],
            'avg_price': orders['avg_price'],
            'score': click_count * 0.7 + orders['total_orders'] * 0.3  # пример скоринга
        })

    # Сортировка по score и берём топ-10
    final.sort(key=lambda x: x['score'], reverse=True)
    top10 = final[:10]

    context['ti'].xcom_push(key='top10', value=top10)

def write_to_redis_bigquery(**context):
    top10 = context['ti'].xcom_pull(key='top10', task_ids='join_orders')
    # Запись в Redis (хэш)
    redis_hook = RedisHook(redis_conn_id='redis_default')
    redis_conn = redis_hook.get_conn()
    redis_conn.hset('top_products', mapping={str(item['product_id']): json.dumps(item) for item in top10})

    # Запись в BigQuery
    hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    for item in top10:
        cursor.execute("""
            INSERT INTO `your-project.dwh.top_products` (product_id, click_count, total_orders, avg_price, score, updated_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """, (item['product_id'], item['click_count'], item['total_orders'], item['avg_price'], item['score']))
    conn.commit()

with DAG(
    'recommendation_pipeline',
    default_args=default_args,
    schedule_interval='*/10 * * * *',  # каждые 10 минут
    catchup=False,
    tags=['example'],
) as dag:

    t1 = PythonOperator(
        task_id='read_kafka',
        python_callable=read_aggregates_from_kafka,
    )

    t2 = PythonOperator(
        task_id='join_orders',
        python_callable=join_with_orders,
    )

    t3 = PythonOperator(
        task_id='write_targets',
        python_callable=write_to_redis_bigquery,
    )

    t1 >> t2 >> t3
