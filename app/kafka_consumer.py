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
    1. Save it to PostgreSQL
    2. Recalculate that user's features instantly
    3. Clear their Redis cache so next request gets fresh data
    """
    user_id = event["user_id"]
    amount = event["amount"]
    country = event["country"]

    # Step 1: Save raw transaction to PostgreSQL
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO raw_transactions (user_id, amount, country)
        VALUES (%s, %s, %s)
    """, (user_id, amount, country))

    # Step 2: Recalculate features for this specific user
    cursor.execute("""
        UPDATE user_features uf
        SET
            transaction_count = t.tx_count,
            avg_transaction_amount = t.avg_amount,
            last_transaction_country = %s,
            risk_score = CASE
                WHEN t.avg_amount > 500 THEN 0.8
                WHEN t.avg_amount > 200 THEN 0.5
                ELSE 0.2
            END
        FROM (
            SELECT
                COUNT(*) as tx_count,
                AVG(amount) as avg_amount
            FROM raw_transactions
            WHERE user_id = %s
        ) t
        WHERE uf.user_id = %s
    """, (country, user_id, user_id))

    conn.commit()
    cursor.close()
    conn.close()

    # Step 3: Clear Redis cache for this user
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    r.delete(f"features:{user_id}")

    print(f"Processed transaction for user {user_id}: ${amount} from {country}")

def start_consumer():
    """
    Starts listening to the transactions topic.
    Runs forever, processing each event as it comes in.
    """
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