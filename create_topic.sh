# Espera a que Kafka esté disponible
echo "Waiting for Kafka to be ready..."
sleep 10

# Crear el topic si no existe
kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic transactions \
  --partitions 1 \
  --replication-factor 1

echo "Topic 'transactions' created"
