#!/bin/bash

# DuckDB Query Service Management Script

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}DuckDB Query Service Management${NC}"
echo "================================"

# Function to start the FastAPI service
start_api() {
    echo -e "${YELLOW}Starting DuckDB Query API...${NC}"
    python query_service.py
}

# Function to start Streamlit interface
start_streamlit() {
    echo -e "${YELLOW}Starting Streamlit Interface...${NC}"
    streamlit run streamlit_app.py --server.host 0.0.0.0 --server.port 8501
}

# Function to run sample queries
run_samples() {
    echo -e "${YELLOW}Running Sample Queries...${NC}"
    python duckdb_engine.py
}

# Function to test connection
test_connection() {
    echo -e "${YELLOW}Testing DuckDB Connection...${NC}"
    python -c "
from duckdb_engine import DuckDBQueryEngine
try:
    engine = DuckDBQueryEngine()
    result = engine.execute_query('SELECT 1 as test')
    print('✅ Connection successful!')
    print(result)
    engine.close_connection()
except Exception as e:
    print(f'❌ Connection failed: {e}')
"
}

# Main menu
case "$1" in
    "api")
        start_api
        ;;
    "streamlit")
        start_streamlit
        ;;
    "samples")
        run_samples
        ;;
    "test")
        test_connection
        ;;
    *)
        echo "Usage: $0 {api|streamlit|samples|test}"
        echo ""
        echo "Commands:"
        echo "  api       - Start the REST API service"
        echo "  streamlit - Start the Streamlit web interface"
        echo "  samples   - Run sample queries"
        echo "  test      - Test DuckDB connection"
        echo ""
        exit 1
        ;;
esac
