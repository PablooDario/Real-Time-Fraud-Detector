FROM python:3.10-slim

# Set working directory inside the container
WORKDIR /app

# Copy requirements first for better caching
COPY ml-serving/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy ML service files
COPY ml-serving/main.py .

# Copy models directory
COPY models/ ./models/

# Expose port for FastAPI
EXPOSE 8000

# Start FastAPI with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]