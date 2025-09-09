import os
import json
import time
import logging 

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from jsonschema import ValidationError, validate, FormatChecker

from generator import transaction_generator

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)
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

class TransactionProducer:
    def __init__(self):
        
        # Kafka Connection parameters
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.topic = os.getenv("KAFKA_TOPIC")
        self.producer_client_id = os.getenv("PRODUCER_CLIENT_ID", "transaction_producer")
        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': self.producer_client_id
        }
                   
        # Wait for Kafka to be ready and ensure topic exists
        self._wait_for_kafka()
        self._ensure_topic_exists()
        
        # Create the kafka producer
        try:
            self.producer = Producer(self.producer_config)
            logger.info("Confluent Kafka Producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Confluent Kafka Producer: {e}")
            raise

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
            
            
    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")
                    
        
    def validate_transaction(self, transaction: dict) -> bool:
        """Validate transaction against JSON schema."""
        try:
            validate(
                instance=transaction,
                schema=TRANSACTION_SCHEMA,
                format_checker=FormatChecker()
            )
            return True
        except ValidationError as e:
            logger.error(f"Invalid transaction: {e.message}")
            return False

    # Use the function created on the generator.py file
    def generate_transaction(self):
        """Generate a valid transaction."""
        transaction = transaction_generator()
        transaction["produced_at"] = time.time()
        return transaction if self.validate_transaction(transaction) else None
    

    def send_transaction(self, transaction):
        """Send a transaction to Kafka."""
        try:
            self.producer.produce(
                self.topic,
                key=transaction["transaction_id"],
                value=json.dumps(transaction),
                callback=self.delivery_report
            )
            self.producer.poll(0)  # Trigger callbacks
        except Exception as e:
            logger.error(f"Failed to send transaction: {e}")
            raise


    def start_streaming(self, interval=1):
        """Start streaming transactions to Kafka."""
        try:
            while True:
                transaction = self.generate_transaction()
                self.send_transaction(transaction)
                print(f"Sent transaction: {transaction}")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("Stopping transaction producer.")
        finally:
            self.producer.flush()
            
if __name__ == "__main__":
    producer = TransactionProducer()
    producer.start_streaming(interval=1)
    
    
# venv\Scripts\Activate.ps1