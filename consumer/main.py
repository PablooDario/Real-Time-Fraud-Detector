from fastapi import FastAPI
import asyncio
from confluent_kafka import Consumer
import json

# Constant Section
KAFKA_BROKER_URL = 'broker:9092'  # Usar puerto interno
KAFKA_TOPIC = 'transaction_topic'
KAFKA_CONSUMER_ID = 'transaction_consumer'

stop_polling_event = asyncio.Event()
app = FastAPI()

def json_deserializer(value):
    if value is None:
        return None
    try:
        return json.loads(value.decode('utf-8'))
    except Exception as e:
        print(f'Unable to decode: {e}')
        return None

# Instance of Kafka Consumer
def create_kafka_consumer():
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER_URL,
        'auto.offset.reset': 'earliest',  # Corregido: puntos en lugar de guiones
        'enable.auto.commit': True,       # Corregido: nombre completo
        'group.id': KAFKA_CONSUMER_ID     # Corregido: puntos en lugar de guiones
        # Removido: value_deserializer no existe en confluent-kafka
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([KAFKA_TOPIC])  # ¡IMPORTANTE! Suscribirse al topic
    return consumer

async def poll_consumer(consumer: Consumer):
    try:
        while not stop_polling_event.is_set():
            print('Polling for messages...')
            
            # Poll por mensajes (timeout en milisegundos)
            message = consumer.poll(timeout=1.0)
            
            if message is None:
                print('No new messages')
                await asyncio.sleep(1)
                continue
            
            if message.error():
                print(f'Consumer error: {message.error()}')
                continue
            
            # Procesar el mensaje
            try:
                # Deserializar el mensaje
                message_data = json.loads(message.value().decode('utf-8'))
                print(f"✅ Received message: {message_data}")
                print(f"   From topic: {message.topic()}")
                print(f"   Partition: {message.partition()}")
                print(f"   Offset: {message.offset()}")
                
            except Exception as e:
                print(f'Error processing message: {e}')
            
            await asyncio.sleep(0.1)  # Pequeña pausa
            
    except Exception as e:
        print(f"Error in consumer: {e}")
    finally:
        print('Closing the consumer')
        consumer.close()

tasklist = []

@app.get('/trigger')
async def trigger_polling():
    if not tasklist:
        stop_polling_event.clear()  # reset flag
        consumer = create_kafka_consumer()
        task = asyncio.create_task(poll_consumer(consumer=consumer))
        tasklist.append(task)
        
        return {"status": "Kafka polling has started"}
    
    return {"status": "Kafka polling already running"}

@app.get('/stop-trigger')
async def stop_trigger():
    stop_polling_event.set()
    if tasklist:
        task = tasklist.pop()
        task.cancel()  # Cancelar la tarea
    
    return {"status": "Kafka polling stopped"}

@app.get('/health')
async def health():
    return {
        "status": "healthy",
        "polling_active": len(tasklist) > 0,
        "kafka_broker": KAFKA_BROKER_URL
    }