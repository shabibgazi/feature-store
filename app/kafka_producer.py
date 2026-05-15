from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_transaction_event(user_id: int, amount: float, country: str):
    event = {
        "user_id": user_id,
        "amount": amount,
        "country": country
    }
    producer.send("transactions", value=event)
    producer.flush()
    print(f"Sent transaction event: {event}")