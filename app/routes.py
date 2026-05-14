import redis
from fastapi import APIRouter, HTTPException
from app.database import get_db_connection
from app.models import UserFeatures, HealthCheck
from psycopg2.extras import RealDictCursor
import json

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