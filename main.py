from fastapi import FastAPI
from app.routes import router
from app.database import init_db

app = FastAPI(title="Feature Store")

@app.on_event("startup")
def startup_event():
    print("Starting up...")
    init_db()
    print("Database initialized!")

app.include_router(router)