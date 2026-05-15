import pandas as pd
import psycopg2
import pickle
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="featurestore",
        user="admin",
        password="password"
    )

def train_model():
    print("Loading data from PostgreSQL...")
    conn = get_db_connection()
    
    df = pd.read_sql("SELECT * FROM credit_transactions", conn)
    conn.close()
    
    print(f"Loaded {len(df)} transactions")

    # Separate features and target
    X = df.drop(columns=["id", "class"])
    y = df["class"]

    # Split into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print(f"Training on {len(X_train)} samples...")
    print(f"Testing on {len(X_test)} samples...")

    # Train the model
    model = RandomForestClassifier(
        n_estimators=100,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # Evaluate the model
    print("\nModel Performance:")
    y_pred = model.predict(X_test)
    print(classification_report(y_test, y_pred))

    # Save the model
    with open("model.pkl", "wb") as f:
        pickle.dump(model, f)
    print("Model saved to model.pkl!")

if __name__ == "__main__":
    train_model()