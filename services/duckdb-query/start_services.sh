#!/bin/bash
set -e

echo "🦆 Starting DuckDB Query Services..."

# Function to handle shutdown
cleanup() {
    echo "Shutting down services..."
    pkill -P $$ 2>/dev/null || true
    exit 0
}

# Set up signal handlers
trap cleanup SIGTERM SIGINT

# Start FastAPI in background
echo "🚀 Starting FastAPI service on port 8002..."
python query_service.py &
FASTAPI_PID=$!

# Wait for FastAPI to be ready
echo "⏳ Waiting for FastAPI to start..."
sleep 10

# Check if FastAPI is running
if ! kill -0 $FASTAPI_PID 2>/dev/null; then
    echo "❌ FastAPI failed to start"
    exit 1
fi

echo "✅ FastAPI service started successfully"

# Start Streamlit in background
echo "🚀 Starting Streamlit interface on port 8501..."
streamlit run streamlit_app.py \
    --server.address 0.0.0.0 \
    --server.port 8501 \
    --server.headless true \
    --browser.gatherUsageStats false &
STREAMLIT_PID=$!

# Wait for Streamlit to be ready
echo "⏳ Waiting for Streamlit to start..."
sleep 5

# Check if Streamlit is running
if ! kill -0 $STREAMLIT_PID 2>/dev/null; then
    echo "❌ Streamlit failed to start"
    kill $FASTAPI_PID 2>/dev/null || true
    exit 1
fi

echo "✅ Streamlit interface started successfully"
echo "📊 Services are ready:"
echo "   - FastAPI: http://localhost:8002"
echo "   - Streamlit: http://localhost:8501"

# Keep both services running
wait $FASTAPI_PID $STREAMLIT_PID
