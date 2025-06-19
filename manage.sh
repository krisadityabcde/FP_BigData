#!/bin/bash
# Utility script for managing the Big Data system (Unix/Linux)

help() {
    echo "Big Data System Management Script"
    echo
    echo "Usage: ./manage.sh [COMMAND]"
    echo
    echo "Commands:"
    echo "  start         Start all services"
    echo "  start-pipeline Start services in proper order for data flow"
    echo "  stop          Stop all services"
    echo "  restart       Restart all services"
    echo "  logs          Show logs for all services"
    echo "  status        Show status of all services"
    echo "  monitor       Monitor MinIO storage"
    echo "  debug         Debug specific service issues"
    echo "  check-data    Check data availability in MinIO"
    echo "  train         Manually trigger ML training"
    echo "  test-data     Test data loading functionality"
    echo "  api-test      Test the prediction API"
    echo "  kafka-ui      Open Kafka UI in browser"
    echo "  minio-ui      Open MinIO Console in browser"
    echo "  clean         Clean up all data and volumes"
    echo "  help          Show this help message"
    echo
}

case "$1" in
    "start")
        echo "🚀 Starting Big Data services..."
        docker-compose up -d --build
        echo "✅ Services started successfully!"
        echo
        echo "🌐 Available services:"
        echo "  - Kafka UI: http://localhost:8080"
        echo "  - MinIO Console: http://localhost:9090"
        echo "  - Prediction API: http://localhost:5001"
        echo
        echo "💡 Use './manage.sh check-data' to verify data availability"
        ;;
    "stop")
        echo "🛑 Stopping Big Data services..."
        docker-compose down
        echo "✅ Services stopped successfully!"
        ;;
    "restart")
        echo "🔄 Restarting Big Data services..."
        docker-compose down
        docker-compose up -d --build
        echo "✅ Services restarted successfully!"
        ;;
    "logs")
        echo "📋 Showing logs for all services..."
        docker-compose logs -f --tail=100
        ;;
    "status")
        echo "📊 Service Status:"
        echo
        docker-compose ps
        echo
        echo "📈 Resource Usage:"
        docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"
        ;;
    "monitor")
        echo "📊 Monitoring MinIO storage..."
        if docker ps --format "{{.Names}}" | grep -q "spark-monitor"; then
            docker exec -it spark-monitor python monitor.py --once
        else
            echo "Starting monitoring container..."
            docker-compose run --rm spark-trainer python monitor.py --once
        fi
        ;;
    "debug")
        echo "🔍 Debugging system issues..."
        echo
        echo "=== Service Status ==="
        docker-compose ps
        echo
        echo "=== Recent Logs ==="
        echo "Data Producer logs:"
        docker-compose logs --tail=10 data-producer
        echo
        echo "Data Consumer logs:"
        docker-compose logs --tail=10 data-consumer
        echo
        echo "Spark Trainer logs:"
        docker-compose logs --tail=10 spark-trainer
        echo
        echo "API logs:"
        docker-compose logs --tail=10 api
        ;;
    "check-data")
        echo "📊 Checking data availability..."
        echo
        echo "=== MinIO Buckets ==="
        docker exec -it minio mc ls minio/ 2>/dev/null || echo "MinIO not accessible"
        echo
        echo "=== Data Files ==="
        docker-compose run --rm data-consumer python -c "
import os
from minio import Minio
try:
    client = Minio('minio:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
    objects = list(client.list_objects('hospital-data', recursive=True))
    print(f'Found {len(objects)} files in hospital-data bucket:')
    for obj in objects[:10]:
        print(f'  - {obj.object_name} ({obj.size} bytes)')
    if len(objects) > 10:
        print(f'  ... and {len(objects)-10} more files')
except Exception as e:
    print(f'Error accessing data: {e}')
"
        ;;
    "train")
        echo "🚀 Manually triggering ML training..."
        docker-compose restart spark-trainer
        echo "Training started. Monitor with: docker-compose logs -f spark-trainer"
        ;;
    "test-data")
        echo "🧪 Testing data loading functionality..."
        docker-compose run --rm spark-trainer python test_data_loading.py
        ;;
    "api-test")
        echo "🧪 Testing Prediction API..."
        echo "Waiting for API to be ready..."
        sleep 5
        docker-compose run --rm api python test_api.py http://api:5001
        ;;
    "start-pipeline")
        echo "🚀 Starting complete data pipeline..."
        echo "1. Starting infrastructure services..."
        docker-compose up -d zookeeper kafka minio kafka-ui
        echo "   Waiting for services to be ready..."
        sleep 15
        
        echo "2. Starting data producer..."
        docker-compose up -d data-producer
        echo "   Waiting for data production to start..."
        sleep 10
        
        echo "3. Starting data consumer..."
        docker-compose up -d data-consumer
        echo "   Waiting for data consumption to start..."
        sleep 15

        echo "4. Starting Streamlit Monitor..."
        cd services/streamlit-monitor
        uv run streamlit run app.py --server.port 8501 &
        echo "   Waiting for Streamlit Monitor to be ready..."
        
        cd ../../
        echo "5. Starting ML trainer..."
        docker-compose up -d spark-trainer
        
        sleep 240
        echo "6. Starting API service..."
        docker-compose up -d api
        
        echo "✅ Complete pipeline started!"
        echo "💡 Monitor progress with: ./manage.sh debug"
        ;;
    "kafka-ui")
        echo "🌐 Opening Kafka UI..."
        if command -v xdg-open &> /dev/null; then
            xdg-open http://localhost:8080
        else
            echo "Please open http://localhost:8080 in your browser"
        fi
        ;;
    "minio-ui")
        echo "🌐 Opening MinIO Console..."
        if command -v xdg-open &> /dev/null; then
            xdg-open http://localhost:9090
        else
            echo "Please open http://localhost:9090 in your browser"
        fi
        ;;
    "clean")
        echo "🧹 Cleaning up system..."
        read -p "This will remove all containers, volumes, and data. Are you sure? (y/N): " confirm
        if [[ "$confirm" =~ ^[Yy]$ ]]; then
            docker-compose down -v --remove-orphans
            docker system prune -f
            echo "✅ System cleaned successfully!"
        else
            echo "❌ Cleanup cancelled"
        fi
        ;;
    "help"|"")
        help
        ;;
    *)
        echo "❌ Unknown command: $1"
        echo
        help
        ;;
esac
