import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_connection():
    connection = psycopg2.connect(
        host="localhost",
        port=5432,
        database="featurestore",
        user="admin",
        password="password"
    )
    return connection

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_features (
            user_id INTEGER PRIMARY KEY,
            transaction_count INTEGER,
            avg_transaction_amount FLOAT,
            last_transaction_country VARCHAR(50),
            risk_score FLOAT
        )
    """)
    
    cursor.execute("""
        INSERT INTO user_features 
            (user_id, transaction_count, avg_transaction_amount, last_transaction_country, risk_score)
        VALUES
            (1, 25, 150.50, 'USA', 0.1),
            (2, 3, 9500.00, 'Russia', 0.9),
            (3, 12, 340.75, 'UK', 0.3)
        ON CONFLICT (user_id) DO NOTHING
    """)
    
    conn.commit()
    cursor.close()
    conn.close()