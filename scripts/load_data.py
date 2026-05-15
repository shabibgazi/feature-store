import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="featurestore",
        user="admin",
        password="password"
    )

def load_data():
    print("Reading CSV...")
    df = pd.read_csv("creditcard.csv")
    print(f"Loaded {len(df)} transactions")

    conn = get_db_connection()
    cursor = conn.cursor()

    # Create transactions table
    print("Creating table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS credit_transactions (
            id SERIAL PRIMARY KEY,
            time FLOAT,
            v1 FLOAT, v2 FLOAT, v3 FLOAT, v4 FLOAT, v5 FLOAT,
            v6 FLOAT, v7 FLOAT, v8 FLOAT, v9 FLOAT, v10 FLOAT,
            v11 FLOAT, v12 FLOAT, v13 FLOAT, v14 FLOAT, v15 FLOAT,
            v16 FLOAT, v17 FLOAT, v18 FLOAT, v19 FLOAT, v20 FLOAT,
            v21 FLOAT, v22 FLOAT, v23 FLOAT, v24 FLOAT, v25 FLOAT,
            v26 FLOAT, v27 FLOAT, v28 FLOAT,
            amount FLOAT,
            class INTEGER
        )
    """)

    # Insert data in batches of 1000
    print("Inserting data...")
    records = df.values.tolist()
    
    execute_batch(cursor, """
        INSERT INTO credit_transactions 
            (time, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10,
             v11, v12, v13, v14, v15, v16, v17, v18, v19, v20,
             v21, v22, v23, v24, v25, v26, v27, v28, amount, class)
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, records, page_size=1000)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Successfully loaded {len(df)} transactions into PostgreSQL!")

if __name__ == "__main__":
    load_data()