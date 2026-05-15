import redis
import json
import pickle
import numpy as np
from fastapi import APIRouter, HTTPException
from app.database import get_db_connection
from app.models import UserFeatures, HealthCheck
from psycopg2.extras import RealDictCursor
from app.kafka_producer import send_transaction_event

# Load the model once when the app starts
with open("model.pkl", "rb") as f:
    fraud_model = pickle.load(f)

router = APIRouter()
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

@router.get("/features/{user_id}", response_model=UserFeatures)
def get_features(user_id: int):
    
    # Step 1: Check Redis first
    cached = redis_client.get(f"features:{user_id}")
    if cached:
        print(f"Cache HIT for user {user_id}")
        return json.loads(cached)
    
    # Step 2: If not in Redis, go to PostgreSQL
    print(f"Cache MISS for user {user_id}")
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute(
        "SELECT * FROM user_features WHERE user_id = %s", 
        (user_id,)
    )
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    
    # Step 3: If user doesn't exist, return 404
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Step 4: Save to Redis for next time
    redis_client.setex(
        f"features:{user_id}",
        300,
        json.dumps(dict(user))
    )
    
    return dict(user)

@router.get("/health", response_model=HealthCheck)
def health_check():
    postgres_ok = False
    redis_ok = False
    
    try:
        conn = get_db_connection()
        conn.close()
        postgres_ok = True
    except:
        pass
    
    try:
        redis_client.ping()
        redis_ok = True
    except:
        pass
    
    return {
        "status": "healthy" if postgres_ok and redis_ok else "unhealthy",
        "postgres": postgres_ok,
        "redis": redis_ok
    }

@router.post("/transaction")
def create_transaction(user_id: int, amount: float, country: str):
    # Send event to Kafka
    send_transaction_event(user_id, amount, country)
    return {"message": f"Transaction event sent for user {user_id}"}

@router.post("/predict")
def predict_fraud(user_id: int):
    # Get the user's latest transaction from PostgreSQL
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute("""
        SELECT time, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10,
               v11, v12, v13, v14, v15, v16, v17, v18, v19, v20,
               v21, v22, v23, v24, v25, v26, v27, v28, amount
        FROM credit_transactions
        WHERE id = %s
    """, (user_id,))
    
    transaction = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if not transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")
    
    # Convert to list and predict
    features = list(transaction.values())
    prediction = fraud_model.predict([features])[0]
    probability = fraud_model.predict_proba([features])[0][1]
    
    return {
        "transaction_id": user_id,
        "is_fraud": bool(prediction),
        "fraud_probability": round(float(probability), 4),
        "risk_level": "HIGH" if probability > 0.7 else "MEDIUM" if probability > 0.3 else "LOW"
    }