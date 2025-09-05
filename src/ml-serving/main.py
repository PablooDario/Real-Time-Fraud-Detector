import os
import joblib
import pandas as pd
import xgboost as xgb
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from sklearn.preprocessing import OneHotEncoder

# Define the app and paths
MODEL_PATH = os.path.join("models", "fraud_xgb.json")
ENCODER_PATH = os.path.join("models", "encoder.pkl")
app = FastAPI(title="Fraud Detection API", version="1.0")

# Input schema
class Transaction(BaseModel):
    transaction_id: str
    user_id: int
    amount: float
    currency: str
    category: str
    timestamp: str
    location: Optional[str] = None
    is_fraud: Optional[bool] = None

# Load model and encoder at startup (GLOBAL LEVEL)
print("Loading model and encoder...")
model = xgb.Booster()
model.load_model(MODEL_PATH)
encoder: OneHotEncoder = joblib.load(ENCODER_PATH)
print("Model and encoder loaded successfully!")

# Preprocessing function
def preprocess(tx: Transaction):
    # Convert to DataFrame
    df = pd.DataFrame([tx.model_dump()])
    df = df.drop(columns=["user_id", "transaction_id", "currency"])

    # Extract hour
    df["hour"] = pd.to_datetime(df["timestamp"]).dt.hour
    df = df.drop(columns=["timestamp"])

    # Handle missing values
    df["location"] = df["location"].fillna("unknown")

    # Encode categorical features (category + location)
    encoded = encoder.transform(df[["category", "location"]])
    encoded_df = pd.DataFrame(
        encoded,
        columns=encoder.get_feature_names_out(["category", "location"]),
        index=df.index
    )

    # Merge numerical + encoded
    df_final = pd.concat([df.drop(columns=["category", "location"]), encoded_df], axis=1)
    
    # Remove target column if present and ensure numeric types
    if 'is_fraud' in df_final.columns:
        df_final = df_final.drop(columns=['is_fraud'])
    
    return df_final.astype('float32')

# Prediction endpoint
@app.post("/predict")
def predict(tx: Transaction):
    try:
        print(f"Received transaction: {tx.transaction_id}")
        
        # Preprocess
        X = preprocess(tx)
        print(f"Preprocessed shape: {X.shape}")

        # Run inference using XGBoost
        dmatrix = xgb.DMatrix(X)
        pred = model.predict(dmatrix)
        print(f"Raw prediction: {pred}")
        
        result = {
            "fraud_prediction": int(pred[0] > 0.5),
            "probability": float(pred[0]),
            "details": "1 = Fraud, 0 = Non-fraud"
        }
        print(f"Returning: {result}")
        return result
        
    except Exception as e:
        print(f"Error in prediction: {e}")
        import traceback
        traceback.print_exc()
        raise

# Health check endpoint
@app.get("/health")
def health():
    return {"status": "healthy"}


# For local debugging only
if __name__ == "__main__":
    # Test with a sample transaction
    example = Transaction(
        transaction_id="81eb959b-22d3-4e31-8378-d37af09c3ac9",
        user_id=1048,
        amount=408.98,
        currency="USD",
        category="Entertainment",
        timestamp="2025-08-28T14:51:42.963443",
        location="BS"
    )
    
    print(predict(example))
    
    # Start server for local development
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)