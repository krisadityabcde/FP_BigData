#!/bin/bash
# Setup script for Big Data Final Project (Unix/Linux)

echo "=== Big Data & Data Lakehouse Final Project Setup ==="
echo

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo "✅ Docker and Docker Compose are installed"

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  .env file not found. Creating from template..."
    cp .env.example .env
    echo "📝 Please edit .env file with your Kaggle credentials before running the system"
    echo "   Get your credentials from: https://www.kaggle.com/settings/account"
    echo
fi

# Create data directory
mkdir -p data
echo "✅ Data directory created"

# Pull Docker images
echo "📥 Pulling Docker images..."
docker-compose pull

echo
echo "🚀 Setup complete! To run the system:"
echo "   1. Edit .env file with your Kaggle credentials"
echo "   2. Run: docker-compose up --build"
echo
echo "🌐 Services will be available at:"
echo "   - Kafka UI: http://localhost:8080"
echo "   - Kafka Broker: localhost:9092"
echo
