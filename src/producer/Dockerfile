FROM python:3.10-slim

# Variables de entorno para evitar buffering y cache en Python
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Creamos directorio de la app
WORKDIR /app

# Copiamos los archivos necesarios
COPY . .
RUN pip install -r requirements.txt

CMD ["python", "producer.py"]