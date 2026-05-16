from kafka import KafkaConsumer
import json
import psycopg2
import redis

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="featurestore",
        user="admin",
        password="password"
    )

def process_transaction(event):
    """
    When a new transaction comes in:
    1. Save it to credit_transactions
    2. Compute features for that transaction
    3. Update user_features
    4. Clear Redis cache
    """
    user_id = event["user_id"]
    amount = event["amount"]

    conn = get_db_connection()
    cursor = conn.cursor()

    # Step 1: Insert into credit_transactions
    cursor.execute("""
        INSERT INTO credit_transactions 
            (time, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10,
             v11, v12, v13, v14, v15, v16, v17, v18, v19, v20,
             v21, v22, v23, v24, v25, v26, v27, v28, amount, class)
        VALUES 
            (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0, %s, 0)
    """, (amount,))

    # Step 2: Update user_features
    cursor.execute("""
        INSERT INTO user_features
            (user_id, transaction_count, avg_transaction_amount,
             last_transaction_country, risk_score)
        VALUES (%s, 1, %s, 'UNKNOWN',
            CASE
                WHEN %s > 500 THEN 0.7
                WHEN %s > 200 THEN 0.4
                ELSE 0.1
            END)
        ON CONFLICT (user_id) DO UPDATE SET
            transaction_count = user_features.transaction_count + 1,
            avg_transaction_amount = (
                user_features.avg_transaction_amount + %s) / 2,
            risk_score = CASE
                WHEN %s > 500 THEN 0.7
                WHEN %s > 200 THEN 0.4
                ELSE 0.1
            END
    """, (user_id, amount, amount, amount, amount, amount, amount))

    conn.commit()
    cursor.close()
    conn.close()

    # Step 3: Clear Redis cache
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    r.delete(f"features:{user_id}")

    print(f"Processed transaction for user {user_id}: ${amount}")

def start_consumer():
    consumer = KafkaConsumer(
        "transactions",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="feature_store_group"
    )
    print("Kafka consumer started. Listening for transactions...")
    for message in consumer:
        event = message.value
        print(f"Received event: {event}")
        process_transaction(event)

if __name__ == "__main__":
    start_consumer()