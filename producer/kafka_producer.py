from confluent_kafka import Producer
from fastapi import HTTPException
from produce_schema import ProduceMessage
import json
import random
import time
import os
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constant Values Section
KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER', 'broker:9092')
KAFKA_TOPIC = 'transaction_topic'
PRODUCER_CLIENT_ID = 'transaction_producer'

conf = {
    'bootstrap.servers': KAFKA_BROKER_URL,
    'client.id': PRODUCER_CLIENT_ID,
    'retries': 5,
    'retry.backoff.ms': 1000,
    'request.timeout.ms': 30000,
    'delivery.timeout.ms': 60000
}

producer = Producer(conf)

def delivery_report(err, msg):
    """Callback para reportar el estado de la entrega del mensaje"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_kafka_message(message: ProduceMessage):
    try:
        # Serializar el mensaje correctamente
        message_data = {
            'message': message.message,  
            'timestamp': time.time()
        }
        
        logger.info(f"Producing message to topic {KAFKA_TOPIC}: {message_data}")
        
        producer.produce(
            KAFKA_TOPIC, 
            value=json.dumps(message_data).encode('utf-8'),
            callback=delivery_report
        )
        
        # Asegurar que todos los mensajes se envíen
        producer.flush(timeout=10)
        
        logger.info("Message sent successfully")
        
    except Exception as error:
        logger.error(f"Error producing message: {error}")
        raise HTTPException(status_code=500, detail=f'Failed to send message: {str(error)}')

def check_kafka_connection():
    """Verificar la conexión con Kafka"""
    try:
        metadata = producer.list_topics(timeout=10)
        logger.info(f"Connected to Kafka. Available topics: {list(metadata.topics.keys())}")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        return False