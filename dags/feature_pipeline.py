from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

def get_db_connection():
    return psycopg2.connect(
        host="feature_store_postgres",
        port=5432,
        database="featurestore",
        user="admin",
        password="password"
    )

def compute_features_from_real_data():
    """
    Reads from credit_transactions and computes
    features for each transaction treated as a user.
    Only processes first 1000 to keep it fast.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Make sure user_features can handle more users
    cursor.execute("""
        INSERT INTO user_features 
            (user_id, transaction_count, avg_transaction_amount, 
             last_transaction_country, risk_score)
        SELECT 
            id as user_id,
            1 as transaction_count,
            amount as avg_transaction_amount,
            'UNKNOWN' as last_transaction_country,
            CASE 
                WHEN class = 1 THEN 0.9
                WHEN amount > 500 THEN 0.7
                WHEN amount > 200 THEN 0.4
                ELSE 0.1
            END as risk_score
        FROM credit_transactions
        WHERE id <= 1000
        ON CONFLICT (user_id) DO UPDATE SET
            avg_transaction_amount = EXCLUDED.avg_transaction_amount,
            risk_score = EXCLUDED.risk_score
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("Computed features from real credit card data!")

def clear_redis_cache():
    """Clears Redis cache for all users"""
    import redis
    r = redis.Redis(host="feature_store_redis", port=6379, decode_responses=True)
    # Clear cache for first 1000 users
    for user_id in range(1, 1001):
        r.delete(f"features:{user_id}")
    print("Redis cache cleared!")

default_args = {
    "owner": "shabib",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="feature_pipeline",
    default_args=default_args,
    description="Nightly pipeline computing features from real credit card data",
    schedule_interval="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    task_compute = PythonOperator(
        task_id="compute_features_from_real_data",
        python_callable=compute_features_from_real_data
    )

    task_clear_cache = PythonOperator(
        task_id="clear_redis_cache",
        python_callable=clear_redis_cache
    )

    task_compute >> task_clear_cache