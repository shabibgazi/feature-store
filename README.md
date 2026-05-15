# Feature Store

A production-grade ML Feature Store built from scratch, capable of serving real-time features with sub-2ms latency using Redis caching and FastAPI.

## What is a Feature Store?

A Feature Store is infrastructure that sits between raw data and ML models. It computes, stores, and serves ML features in both batch and real-time modes — eliminating redundant feature computation across teams and ensuring consistency between training and serving.

## Architecture
Transaction Event
↓
Kafka Producer
↓
Kafka Consumer ──→ PostgreSQL (permanent storage)
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


## Tech Stack
| Tool | Role |
|------|------|
| **FastAPI** | REST API layer for feature serving |
| **PostgreSQL** | Permanent feature storage |
| **Redis** | Sub-2ms feature caching |
| **Apache Kafka** | Real-time transaction event streaming |
| **Apache Airflow** | Nightly batch feature pipeline |
| **Docker** | Containerized infrastructure |
| **Python** | Core application logic |

## Performance

- **Cache HIT latency:** 1.83ms (Redis)
- **Cache MISS latency:** 24.85ms (PostgreSQL)
- **Redis is 13.6x faster** than direct PostgreSQL lookups
- **Cache TTL:** 5 minutes (auto-expiry for feature freshness)

## How It Works

### Real-Time Updates (Kafka)
When a transaction occurs, a Kafka event is fired instantly. The consumer processes it in milliseconds — updating PostgreSQL and clearing the Redis cache so the next request gets fresh features.

### Batch Updates (Airflow)
Every night at midnight, an Airflow DAG runs a full pipeline:
1. Generates/ingests raw transactions
2. Recalculates all user features
3. Clears Redis cache

### Feature Serving (FastAPI + Redis)
Every feature request checks Redis first. On a cache hit, features are returned in under 2ms. On a miss, features are fetched from PostgreSQL, cached in Redis, and returned.

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/features/{user_id}` | Get features for a user |
| POST | `/transaction` | Send a new transaction event |
| GET | `/health` | Health check for all services |

## Features Stored Per User

| Feature | Description |
|---------|-------------|
| `transaction_count` | Total number of transactions |
| `avg_transaction_amount` | Average spend amount |
| `last_transaction_country` | Most recent transaction country |
| `risk_score` | Computed fraud risk (0.0 - 1.0) |

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

4. Start the API:
\```bash
uvicorn main:app --reload
\```

5. Start the Kafka consumer:
\```bash
python -m app.kafka_consumer
\```

6. Visit the API docs:
\```
http://127.0.0.1:8000/docs
\```