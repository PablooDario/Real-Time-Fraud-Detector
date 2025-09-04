FROM python:3.10-slim

WORKDIR /app

# Copy all and install the python libs
COPY . . 
RUN pip install -r requirements.txt

# Run the Consumer
CMD ["python", "consumer.py"]