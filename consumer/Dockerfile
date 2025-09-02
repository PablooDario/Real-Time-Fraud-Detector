FROM python:3.10-slim

# Variables de entorno para evitar buffering y cache en Python
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Creamos directorio de la app
WORKDIR /app

# Copiamos los archivos necesarios
COPY main.py /app/

# Instalar dependencias
RUN pip install --no-cache-dir fastapi uvicorn confluent-kafka pydantic

# Exponer puerto 8001 para FastAPI
EXPOSE 8001

# Comando para correr la app con Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]