# Feature Store

A production-grade ML Feature Store built from scratch, serving real-time fraud detection features with sub-2ms latency.

## What is a Feature Store?

A Feature Store is infrastructure that sits between raw data and ML models. It computes, stores, and serves ML features in both batch and real-time modes — eliminating redundant feature computation and ensuring consistency between training and serving.

## Architecture
Transaction Event
↓
Kafka Producer
↓
Kafka Consumer ──→ PostgreSQL (284,807 real transactions)
↓
Airflow Nightly Pipeline
(recalculates all features)
↓
FastAPI Server
↓
Check Redis Cache first
↙                    ↘
Cache HIT              Cache MISS
(1.83ms)                (24.85ms)
↓                        ↓
Return instantly        Fetch from PostgreSQL
Store in Redis
Return features
↓
Random Forest Model
(94% F1 on fraud cases)

## Tech Stack

| Tool | Role |
|------|------|
| **FastAPI** | REST API layer for feature serving |
| **PostgreSQL** | Permanent storage for 284,807 transactions |
| **Redis** | Sub-2ms feature caching |
| **Apache Kafka** | Real-time transaction event streaming |
| **Apache Airflow** | Nightly batch feature pipeline |
| **Scikit-learn** | Random Forest fraud detection model |
| **Docker** | Containerized infrastructure |
| **Python** | Core application logic |

## Performance

- **Cache HIT latency:** 1.83ms (Redis)
- **Cache MISS latency:** 24.85ms (PostgreSQL)
- **Redis is 13.6x faster** than direct PostgreSQL lookups
- **Fraud detection accuracy:** 100% overall, 94% F1 on fraud cases
- **Dataset:** 284,807 real credit card transactions (492 fraud cases)
- **Cache TTL:** 5 minutes (auto-expiry for feature freshness)

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/features/{user_id}` | Get features for a user |
| POST | `/transaction` | Send a new transaction event |
| POST | `/predict` | Predict fraud probability for a transaction |
| GET | `/health` | Health check for all services |

## How It Works

### Real-Time Updates (Kafka)
When a transaction occurs, a Kafka event is fired instantly. The consumer processes it in milliseconds — updating PostgreSQL and clearing the Redis cache so the next request gets fresh features.

### Batch Updates (Airflow)
Every night at midnight, an Airflow DAG runs a full pipeline:
1. Ingests raw transactions
2. Recalculates all user features
3. Clears Redis cache

### Feature Serving (FastAPI + Redis)
Every feature request checks Redis first. On a cache hit, features are returned in under 2ms. On a miss, features are fetched from PostgreSQL, cached in Redis, and returned.

### Fraud Detection (Random Forest)
A Random Forest model trained on 284,807 real transactions returns:
- `is_fraud` — true or false
- `fraud_probability` — confidence score (0.0 to 1.0)
- `risk_level` — HIGH, MEDIUM, or LOW

## Running Locally

### Prerequisites
- Docker
- Python 3.12+

### Steps

1. Clone the repo:
\```bash
git clone https://github.com/shabibgazi/feature-store.git
cd feature-store
\```

2. Start all containers:
\```bash
docker compose up
\```

3. Create virtual environment and install dependencies:
\```bash
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
\```

4. Download the dataset from Kaggle:
\```
https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud
\```
Place `creditcard.csv` in the root folder.

5. Load the dataset:
\```bash
python scripts/load_data.py
\```

6. Train the model:
\```bash
python scripts/train_model.py
\```

7. Start the API:
\```bash
uvicorn main:app --reload
\```

8. Start the Kafka consumer:
\```bash
python -m app.kafka_consumer
\```

9. Visit the API docs:
\```
http://127.0.0.1:8000/docs
\```