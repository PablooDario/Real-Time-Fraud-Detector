import os
import json
import time
import logging 
import requests
from redis import Redis

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from jsonschema import ValidationError, validate, FormatChecker

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)

# Create a logger with the curretn module
logger = logging.getLogger(__name__)

# JSON Schema for transaction validation
TRANSACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "transaction_id": {"type": "string"},
        "user_id": {"type": "integer"},
        "amount": {"type": "number"},
        "currency": {"type": "string"},
        "category": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "location": {"type": "string"},
        "is_fraud": {"type": "integer"},
        "produced_at": {"type": "number"}
    },
    "required": ["transaction_id", "user_id", "amount",
                 "currency", "timestamp", "is_fraud"]
}

class TransactionConsumer():
    def __init__(self):
        
        # Kafka Connection parameters
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.topic = os.getenv("KAFKA_TOPIC")
        self.consumer_client_id = os.getenv("CONSUMER_CLIENT_ID", "transaction_consumer")
        self.group_id = os.getenv("KAFKA_GROUP_ID", "transaction_consumer_group")
        
        self.consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': self.consumer_client_id,
            'group.id': self.group_id,
            'enable.auto.commit': False
        }
                   
        # Wait for Kafka to be ready and ensure topic exists
        self._wait_for_kafka()
        self._ensure_topic_exists()
        
        # Create the kafka consumer and subscribe to the topic
        try:
            self.consumer = Consumer(self.consumer_config)
            self.consumer.subscribe([self.topic])
            logger.info("Confluent Kafka Consumer initialized and connected successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Confluent Kafka Consumer: {e}")
            raise
        
        # Initialize Redis connection
        self.redis_client = Redis(host="redis", port=6379, decode_responses=True)

    def _wait_for_kafka(self, max_retries=30):
        """Wait for Kafka to be available."""
        for attempt in range(max_retries):
            try:
                admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
                metadata = admin_client.list_topics(timeout=5)
                logger.info(f"Connected to Kafka. Available topics: {list(metadata.topics.keys())}")
                return
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries}: Waiting for Kafka... {e}")
                time.sleep(2)
        
        raise Exception("Could not connect to Kafka after maximum retries")

    def _ensure_topic_exists(self):
        try:
            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
            metadata = admin_client.list_topics(timeout=10)
            if self.topic not in metadata.topics:
                logger.info(f"Topic '{self.topic}' not found.")
                
        except Exception as e:
            logger.error(f"Error ensuring topic exists: {e}")
            raise
        
    def store_redis(self, tx):
        try:
            self.redis_client.lpush("transactions", json.dumps(tx))
            logger.info(f"Stored transaction in Redis: {tx}")
        except Exception as e:
            logger.error(f"Failed to store transaction in Redis: {e}")
    
    def predict(self, tx_data):
        try:
            # Send transaction to ML model for prediction
            response = requests.post("http://ml-serving:8000/predict", json=tx_data)
            prediction = response.json()

            logger.info(f"Prediction response: {prediction}")
            enriched_tx = tx_data.copy()
            enriched_tx["is_fraud_predicted"] = prediction.get("fraud_prediction")
            enriched_tx["fraud_probability"] = prediction.get("probability")
            
            logger.info(f"Enriched transaction: {enriched_tx}")
            
            # Store the enriched transaction in Redis
            self.store_redis(enriched_tx)

        except requests.exceptions.ConnectionError:
            logger.error("❌ Could not connect to ML service")
        except requests.exceptions.Timeout:
            logger.error("❌ ML service request timed out")
        except ValueError as e:
            logger.error(f"❌ Invalid JSON response: {e}")
            logger.error(f"Response text: {response.text if 'response' in locals() else 'No response'}")
        except Exception as e:
            logger.error(f"❌ Prediction failed: {e}")
        
    def consume_messages(self):
        while True:
            # Poll Kafka for a new message (wait up to 1 second)
            msg = self.consumer.poll(1.0)
            # If no message was received, continue to the next iteration
            if msg is None:
                continue

            # If there's an error with the message, log it and skip
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                # Try to parse and validate the message as JSON against the schema
                tx_data = json.loads(msg.value().decode("utf-8"))
                validate(instance=tx_data, schema=TRANSACTION_SCHEMA, format_checker=FormatChecker())
                
                arrival_time = time.time()
                latency_ms = (arrival_time - tx_data["produced_at"]) * 1000
                
                logger.info(f"Valid Message received from {msg.topic()} [{msg.partition()}], latency: {latency_ms:.2f} ms")
                logger.info(f"Message: {tx_data}")
                # Predict whether the transaction is fraudulent
                self.predict(tx_data)
                
                # Commit the offset for the message (mark it as processed)
                self.consumer.commit(msg)
            
            except json.JSONDecodeError as e:
                logger.warning(f"Malformed JSON: {e}")
            except ValidationError as e:
                logger.warning(f"Invalid JSON according to schema: {e.message}")



if __name__ == "__main__":
    consumer = TransactionConsumer()
    consumer.consume_messages()
