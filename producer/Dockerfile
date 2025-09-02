FROM python:3.10-slim

# Variables de entorno para evitar buffering y cache en Python
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Creamos directorio de la app
WORKDIR /app

# Copiamos los archivos necesarios
COPY main.py kafka_producer.py produce_schema.py /app/

# Instalar dependencias
RUN pip install --no-cache-dir fastapi uvicorn confluent-kafka pydantic

# Exponer puerto 8000 para FastAPI
EXPOSE 8000

# Comando para correr la app con Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
