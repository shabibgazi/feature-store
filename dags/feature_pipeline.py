from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import random

def get_db_connection():
    return psycopg2.connect(
        host="feature_store_postgres",
        port=5432,
        database="featurestore",
        user="admin",
        password="password"
    )

def generate_raw_transactions():
    """
    Simulates new transactions coming in.
    In a real company this would read from Kafka or S3.
    Here we just generate fake data.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create raw transactions table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_transactions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER,
            amount FLOAT,
            country VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Generate fake transactions for our 3 users
    countries = ['USA', 'UK', 'Canada', 'Russia', 'Germany']
    for user_id in [1, 2, 3]:
        for _ in range(random.randint(1, 5)):
            amount = round(random.uniform(10, 1000), 2)
            country = random.choice(countries)
            cursor.execute(
                "INSERT INTO raw_transactions (user_id, amount, country) VALUES (%s, %s, %s)",
                (user_id, amount, country)
            )

    conn.commit()
    cursor.close()
    conn.close()
    print("Generated raw transactions successfully!")

def update_user_features():
    """
    Reads raw transactions and recalculates features for each user.
    This is what dbt would do in a real pipeline.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Recalculate features from raw transactions
    cursor.execute("""
        UPDATE user_features uf
        SET
            transaction_count = t.tx_count,
            avg_transaction_amount = t.avg_amount,
            last_transaction_country = t.last_country,
            risk_score = CASE
                WHEN t.avg_amount > 500 THEN 0.8
                WHEN t.avg_amount > 200 THEN 0.5
                ELSE 0.2
            END
        FROM (
            SELECT
                user_id,
                COUNT(*) as tx_count,
                AVG(amount) as avg_amount,
                (SELECT country FROM raw_transactions r2
                 WHERE r2.user_id = r1.user_id
                 ORDER BY created_at DESC LIMIT 1) as last_country
            FROM raw_transactions r1
            GROUP BY user_id
        ) t
        WHERE uf.user_id = t.user_id
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("Updated user features successfully!")

def clear_redis_cache():
    """
    Clears Redis cache so next API request gets fresh features.
    """
    import redis
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    for user_id in [1, 2, 3]:
        r.delete(f"features:{user_id}")
    print("Redis cache cleared successfully!")

# Define the DAG
default_args = {
    "owner": "shabib",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="feature_pipeline",
    default_args=default_args,
    description="Nightly pipeline to update user features",
    schedule_interval="0 0 * * *",  # runs every night at midnight
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    task_generate = PythonOperator(
        task_id="generate_raw_transactions",
        python_callable=generate_raw_transactions
    )

    task_update = PythonOperator(
        task_id="update_user_features",
        python_callable=update_user_features
    )

    task_clear_cache = PythonOperator(
        task_id="clear_redis_cache",
        python_callable=clear_redis_cache
    )

    # This defines the order: generate → update → clear cache
    task_generate >> task_update >> task_clear_cache