from pydantic import BaseModel
from typing import Optional

class UserFeatures(BaseModel):
    user_id: int
    transaction_count: int
    avg_transaction_amount: float
    last_transaction_country: str
    risk_score: float

class HealthCheck(BaseModel):
    status: str
    postgres: bool
    redis: bool