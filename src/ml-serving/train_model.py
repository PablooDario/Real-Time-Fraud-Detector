import os
import pandas as pd
import xgboost as xgb
import joblib

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import (
    classification_report, confusion_matrix, 
    f1_score, recall_score, precision_score
)


def data_preprocessing(data_path: str):
    """Load and preprocess transaction data for fraud detection."""
    data = pd.read_csv(data_path)

    # Drop irrelevant columns
    data = data.drop(columns=["user_id", "transaction_id", "currency"])

    # Extract time-based features
    data["hour"] = pd.to_datetime(data["timestamp"]).dt.hour
    data = data.drop(columns=["timestamp"])

    # Handle missing values
    data["location"] = data["location"].fillna("unknown")

    # One-hot encode categorical features
    encoder = OneHotEncoder(sparse_output=False, handle_unknown="ignore")
    encoded_data = pd.DataFrame(
        encoder.fit_transform(data[["category", "location"]]),
        columns=encoder.get_feature_names_out(["category", "location"]),
        index=data.index
    )
    
    # Save the encoder
    encoder_path = os.path.join("..", "models", "encoder.pkl")
    joblib.dump(encoder, encoder_path)

    # Combine features
    data_final = pd.concat([data.drop(columns=["category", "location"]), encoded_data], axis=1)

    y = data_final["is_fraud"]
    X = data_final.drop("is_fraud", axis=1)
    
    return train_test_split(X, y, test_size=0.2, random_state=42)


def train_model(X_train, y_train):
    """Train an XGBoost model with best hyperparameters."""
    model = xgb.XGBClassifier(
        colsample_bytree=1.0,
        gamma=0.1,
        learning_rate=0.1,
        max_depth=5,
        min_child_weight=1,
        n_estimators=500,
        scale_pos_weight=25,
        subsample=1.0,
        eval_metric="logloss"
    )
    model.fit(X_train, y_train)
    return model


def eval_model(model, X_test, y_test):
    """Evaluate the trained model and print metrics."""
    y_pred = model.predict(X_test)
    print(f"Recall on fraud class: {recall_score(y_test, y_pred, pos_label=1):.4f}")
    print(f"Precision on fraud class: {precision_score(y_test, y_pred, pos_label=1):.4f}")
    print(f"F1-score on fraud class: {f1_score(y_test, y_pred, pos_label=1):.4f}")
    print("Confusion Matrix:")
    print(confusion_matrix(y_test, y_pred))
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
          
    
def save_model(model, model_path):
    """Save model to disk as json."""
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    # Use XGBoost native ONNX export
    model.save_model(model_path)
    print(f"-----Model saved-----")


if __name__ == "__main__":
    data_path = os.path.join("..", "..", "data", "synthetic_transactions.csv")
    model_path = os.path.join("..", "models", "fraud_xgb.json")

    # Preprocess data
    X_train, X_test, y_train, y_test = data_preprocessing(data_path)

    # Train model
    model = train_model(X_train, y_train)

    # Evaluate model
    eval_model(model, X_test, y_test)

    # Save trained model as ONNX
    save_model(model, model_path)
