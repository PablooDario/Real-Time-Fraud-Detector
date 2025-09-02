from fastapi import FastAPI, BackgroundTasks
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException
from kafka_producer import produce_kafka_message
from contextlib import asynccontextmanager
from produce_schema import ProduceMessage
import logging

# Constant Variable Section
KAFKA_BROKER_URL = 'broker:9092'
KAFKA_TOPIC = 'transaction_topic'
KAFKA_ADMIN_CLIENT = 'fastapi-admin-client'

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ✅ Configuración correcta para confluent-kafka AdminClient
    admin_config = {
        'bootstrap.servers': KAFKA_BROKER_URL,
        'client.id': KAFKA_ADMIN_CLIENT
    }
    
    admin_client = AdminClient(admin_config)
    
    try:
        # ✅ Verificar si el topic existe
        metadata = admin_client.list_topics(timeout=10)
        
        if KAFKA_TOPIC not in metadata.topics:
            print(f"Topic '{KAFKA_TOPIC}' not found. Creating...")
            
            # ✅ Crear el topic
            topic_list = [NewTopic(
                topic=KAFKA_TOPIC,
                num_partitions=1,
                replication_factor=1
            )]
            
            fs = admin_client.create_topics(topic_list)
            
            # Esperar a que se complete la creación
            for topic, f in fs.items():
                try:
                    f.result()  # Esperar el resultado
                    print(f"Topic '{topic}' created successfully")
                except KafkaException as e:
                    print(f"Failed to create topic '{topic}': {e}")
                except Exception as e:
                    print(f"Unexpected error creating topic '{topic}': {e}")
        else:
            print(f"Topic '{KAFKA_TOPIC}' already exists")
            
    except Exception as e:
        print(f"Error during admin operations: {e}")
        logging.error(f"Kafka admin error: {e}")
    
    yield  # Separation point
    
    # Cleanup si es necesario
    print("Shutting down...")

app = FastAPI(lifespan=lifespan)

@app.post("/produce/message", tags=["Produce Message"])
async def produce_message(messageRequest: ProduceMessage, background_tasks: BackgroundTasks):
    background_tasks.add_task(produce_kafka_message, messageRequest)
    return {'message': 'Message Received'}

@app.get("/health")
async def health():
    return {"status": "healthy"}

# Para ejecutar:
# source venv/bin/activate
# uvicorn main:app --reload --host 0.0.0.0 --port 8000