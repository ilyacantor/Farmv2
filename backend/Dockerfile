FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Environment variable for port (cloud platforms set this)
ENV PORT=8080

# Run the application
CMD uvicorn src.main:app --host 0.0.0.0 --port $PORT
