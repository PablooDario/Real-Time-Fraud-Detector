# Use Python base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Create static directory for any additional files
RUN mkdir -p static

# Copy dashboard files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port
EXPOSE 8501

# Start the dashboard
CMD ["uvicorn", "dashboard:app", "--host", "0.0.0.0", "--port", "8501"]