import random
import os
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Configuration - Increased fraud sets for higher fraud rate
COMPROMISED_USERS = set(random.sample(range(0, 2999), 200))  # 200 compromised users
HIGH_RISK_CATEGORIES = ["Luxury Goods", "Cryptocurrency", "High-End Electronics", "Night Clubs", "Gambling", "Online Shopping"]
FREQUENT_USERS = set(random.sample(range(0, 2999), 300))  # Users who make frequent transactions

normal_categories = [
    "Groceries", "Electronics", "Clothing", "Restaurants", "Gas Stations", "Travel",
    "Entertainment", "Subscriptions", "Utilities", "Books", "Sports Equipment", "Toys",
    "Furniture", "Jewelry", "Pet Supplies", "Home Improvement", "Parking", "Public Transport",
    "Gifts", "Coffee Shops", "Health & Fitness", "Education", "Charity", "Pharmacy"
]

all_categories = normal_categories + HIGH_RISK_CATEGORIES

weights = [
    10, 6, 6, 9, 9, 10, 5, 8, 7, 6,
    9, 6, 4, 3, 3, 3, 3, 4, 5, 8,
    4, 8, 6, 5, 3, 2, 2, 3, 2, 2
]


def transaction_generator():
    """Generate a synthetic transaction with possible fraud patterns."""

    # Generate base transaction
    transaction = {
        "transaction_id": fake.uuid4(),
        "user_id": random.randint(0, 2999),
        # More realistic amount distribution
        "amount" : round(random.lognormvariate(4, 1.5), 2),  # Log-normal for more realistic distribution
        "currency" : "USD",
        "category" : random.choices(all_categories, weights=weights, k=1)[0],
        # Add some historical timestamps
        "timestamp": (datetime.utcnow() - timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )).isoformat(),
        "location": fake.country_code(),
        "is_fraud": 0
    }

    is_fraud = 0
    user_id = transaction["user_id"]
    amount = transaction["amount"]
    category = transaction["category"]

    # --- Fraud patterns ---

    # 1. Account takeover: compromised users making large or unusual payments
    if user_id in COMPROMISED_USERS:
        if amount > 800 and random.random() < 0.4:  # 40% probability of fraud
            is_fraud = 1
            transaction["amount"] = round(random.uniform(800, 5000), 2)
            transaction["category"] = random.choice(HIGH_RISK_CATEGORIES)
        # Also flag smaller but suspicious amounts from compromised accounts
        elif amount > 200 and random.random() < 0.2:
            is_fraud = 1
            transaction["location"] = random.choice(["CN", "RU", "NG", "RO"])

    # 2. Card testing: very small repeated charges (more aggressive)
    if not is_fraud and amount < 5:
        if random.random() < 0.35:  # 35% probability of fraud
            is_fraud = 1
            transaction["amount"] = round(random.uniform(0.01, 4.99), 2)
            transaction["category"] = random.choice(HIGH_RISK_CATEGORIES)
            transaction["location"] = random.choice(["US", "CA", "GB"])

    # 3. High-risk category (more realistic thresholds)
    if not is_fraud and any(risk_category in category for risk_category in HIGH_RISK_CATEGORIES):
        if amount > 1000 and random.random() < 0.45:  # Increased probability
            is_fraud = 1
            transaction["amount"] = round(random.uniform(1000, 3000), 2)
        # Also flag smaller suspicious amounts at risky category
        elif amount > 300 and random.random() < 0.25:
            is_fraud = 1

    # 4. Geographic anomaly: users spending from high-risk regions
    if not is_fraud:
        if user_id % 200 == 0 and random.random() < 0.3:  # More users, higher probability
            is_fraud = 1
            transaction["location"] = random.choice(["CN", "RU", "NG", "RO", "PK", "BD"])
            # Make amount more suspicious for geographic anomalies
            transaction["amount"] = round(random.uniform(500, 2500), 2)

    # 5. Unusual time patterns (transactions at odd hours)
    if not is_fraud:
        transaction_time = datetime.fromisoformat(transaction["timestamp"])
        if transaction_time.hour in [1, 2, 3, 4, 5] and amount > 500 and random.random() < 0.15:
            is_fraud = 1
            transaction["amount"] = round(random.uniform(500, 2000), 2)

    # 6. Velocity fraud (rapid transactions - simulated by user patterns)
    if not is_fraud and user_id in FREQUENT_USERS:
        if amount > 1500 and random.random() < 0.2:
            is_fraud = 1
            transaction["category"] = random.choice(all_categories)

    # 7. Round number fraud (fraudsters often use round amounts)
    if not is_fraud and amount % 100 == 0 and amount >= 1000:
        if random.random() < 0.15:
            is_fraud = 1
            transaction["location"] = random.choice(["CN", "RU", "NG"])

    # 8. Random fraud to reach target rate (catch-all pattern)
    if not is_fraud and random.random() < 0.008:  # Small random fraud rate
        is_fraud = 1
        # Make it look like a typical fraud case
        transaction["amount"] = round(random.uniform(100, 1000), 2)
        if random.random() < 0.5:
            transaction["category"] = random.choice(HIGH_RISK_CATEGORIES)
        else:
            transaction["location"] = random.choice(["CN", "RU", "NG", "RO"])

    transaction["is_fraud"] = is_fraud
    return transaction


if __name__ == "__main__":
    # Number of transactions to generate for offline dataset
    num_transactions = 10000
    print(f'Generating {num_transactions} transactions')
    transactions = [transaction_generator() for _ in range(num_transactions)]
    df = pd.DataFrame(transactions)
    
    # Calculate and display fraud rate
    fraud_count = df['is_fraud'].sum()
    fraud_rate = (fraud_count / num_transactions) * 100
    
    data_dir = os.path.join("..", "..", "data")
    os.makedirs(data_dir, exist_ok=True)

    data_path = os.path.join(data_dir, "synthetic_transactions.csv")
    df.to_csv(data_path, index=False)
    
    print(f"Generated {num_transactions} transactions.")
    print(f"Fraud transactions: {fraud_count} ({fraud_rate:.2f}%)")
    print(f"Legitimate transactions: {num_transactions - fraud_count}")