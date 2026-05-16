from fastapi import FastAPI
from app.routes import router
from app.database import init_db
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(title="Feature Store")

# Add Prometheus metrics
Instrumentator().instrument(app).expose(app)

@app.on_event("startup")
def startup_event():
    print("Starting up...")
    init_db()
    print("Database initialized!")

app.include_router(router)