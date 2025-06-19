FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    procps \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Convert line endings and make startup script executable
RUN dos2unix start_services.sh && chmod +x start_services.sh

# Expose ports
EXPOSE 8002 8501

# Run the startup script
CMD ["./start_services.sh"]
