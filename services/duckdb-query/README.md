# DuckDB Query Service

A comprehensive DuckDB-based query service for analyzing Parquet files stored in MinIO. This service provides both REST API and interactive web interface for querying data.

## Features

- **Direct Parquet Querying**: Query Parquet files directly from MinIO using DuckDB
- **REST API**: FastAPI-based service for programmatic access
- **Web Interface**: Streamlit-based interactive interface for data exploration
- **Schema Analysis**: Get detailed schema information about your Parquet files
- **View Management**: Create and manage views for easier querying
- **Query Optimization**: Built-in query limits and optimization features

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Streamlit     │    │   FastAPI       │    │     DuckDB      │
│  Web Interface  │───▶│  Query Service  │───▶│  Query Engine   │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────┐
                                              │     MinIO       │
                                              │ Parquet Storage │
                                              │                 │
                                              └─────────────────┘
```

## Components

### 1. DuckDB Engine (`duckdb_engine.py`)
- Core DuckDB connection and query execution
- MinIO S3 configuration
- Parquet file reading capabilities
- View management
- Schema inspection

### 2. FastAPI Service (`query_service.py`)
- REST API endpoints for querying
- Health checks and monitoring
- Error handling and logging
- Query result formatting

### 3. Streamlit Interface (`streamlit_app.py`)
- Interactive web interface
- Query builder and execution
- Data visualization
- Schema exploration
- Sample queries and templates

## Installation

### Prerequisites
- Python 3.9+
- MinIO server running
- Docker (optional, for containerized deployment)

### Local Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your MinIO configuration
```

3. Start the services:
```bash
# Start REST API
python query_service.py

# Start Streamlit interface (in another terminal)
streamlit run streamlit_app.py
```

### Docker Deployment

1. Build the Docker image:
```bash
docker build -t duckdb-query-service .
```

2. Run the container:
```bash
docker run -p 8002:8002 -p 8501:8501 \
  -e MINIO_ENDPOINT=http://your-minio:9000 \
  -e MINIO_ACCESS_KEY=your-access-key \
  -e MINIO_SECRET_KEY=your-secret-key \
  duckdb-query-service
```

## Usage

### REST API Endpoints

#### Health Check
```http
GET /health
```

#### Query Parquet File
```http
POST /query/parquet
```
Parameters:
- `bucket_name`: MinIO bucket name
- `file_path`: Path to Parquet file
- `query`: SQL query (optional, use `{table}` as placeholder)
- `limit`: Maximum rows to return

Example:
```bash
curl -X POST "http://localhost:8002/query/parquet" \
  -H "Content-Type: application/json" \
  -d '{
    "bucket_name": "data-lake",
    "file_path": "processed/sales_data.parquet",
    "query": "SELECT * FROM {table} WHERE amount > 1000 ORDER BY date DESC",
    "limit": 100
  }'
```

#### Get Schema Information
```http
GET /schema/{bucket_name}?file_path={file_path}
```

#### Create View
```http
POST /view/create
```
Parameters:
- `view_name`: Name for the view
- `bucket_name`: MinIO bucket name
- `file_path`: Path to Parquet file

#### Query View
```http
POST /query/view
```
Parameters:
- `view_name`: Name of the view
- `query`: SQL query (optional)
- `limit`: Maximum rows to return

### Web Interface

Access the Streamlit interface at `http://localhost:8501`

Features:
- **Query Data**: Execute SQL queries against Parquet files
- **Schema Info**: View detailed schema information
- **Quick Analysis**: Perform statistical analysis and visualization
- **Sample Queries**: Pre-built query templates

### Sample Queries

#### Basic Data Exploration
```sql
-- Get all data
SELECT * FROM {table} LIMIT 100;

-- Count total rows
SELECT COUNT(*) as total_rows FROM {table};

-- Get unique values in a column
SELECT DISTINCT column_name FROM {table};
```

#### Aggregations
```sql
-- Group by analysis
SELECT 
    category, 
    COUNT(*) as count,
    AVG(amount) as avg_amount,
    SUM(amount) as total_amount
FROM {table} 
GROUP BY category 
ORDER BY total_amount DESC;
```

#### Filtering and Sorting
```sql
-- Filter with conditions
SELECT * FROM {table} 
WHERE date >= '2023-01-01' 
  AND amount > 1000 
  AND status = 'completed'
ORDER BY date DESC
LIMIT 50;
```

#### Time Series Analysis
```sql
-- Daily aggregations
SELECT 
    DATE_TRUNC('day', timestamp_column) as date,
    COUNT(*) as daily_count,
    AVG(value_column) as daily_avg
FROM {table}
WHERE timestamp_column >= '2023-01-01'
GROUP BY DATE_TRUNC('day', timestamp_column)
ORDER BY date;
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MINIO_ENDPOINT` | MinIO server endpoint | `http://minio:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO secret key | `minioadmin` |
| `QUERY_SERVICE_HOST` | API service host | `0.0.0.0` |
| `QUERY_SERVICE_PORT` | API service port | `8002` |
| `STREAMLIT_HOST` | Streamlit host | `0.0.0.0` |
| `STREAMLIT_PORT` | Streamlit port | `8501` |

### DuckDB Configuration

The service automatically configures DuckDB with:
- `httpfs` extension for S3/MinIO access
- S3 endpoint configuration
- SSL and authentication settings
- Path-style URL configuration for MinIO compatibility

## Management Scripts

### Linux/Mac
```bash
# Start API service
./run.sh api

# Start Streamlit interface
./run.sh streamlit

# Run sample queries
./run.sh samples

# Test connection
./run.sh test
```

### Windows
```cmd
# Start API service
run.bat api

# Start Streamlit interface
run.bat streamlit

# Run sample queries
run.bat samples

# Test connection
run.bat test
```

## Performance Considerations

1. **Query Limits**: Default limit of 1000 rows, maximum 10,000
2. **Memory Usage**: DuckDB uses in-memory processing for fast queries
3. **Parquet Optimization**: DuckDB efficiently reads Parquet files with column pruning
4. **Connection Pooling**: Single connection per service instance
5. **Caching**: Consider implementing query result caching for frequently accessed data

## Troubleshooting

### Common Issues

1. **MinIO Connection Failed**
   - Check MinIO endpoint configuration
   - Verify access credentials
   - Ensure MinIO is accessible from the service

2. **Parquet File Not Found**
   - Verify bucket name and file path
   - Check file permissions
   - Ensure file exists in MinIO

3. **DuckDB Extension Error**
   - Ensure `httpfs` extension is properly installed
   - Check DuckDB version compatibility

4. **Memory Issues**
   - Reduce query limits
   - Use column selection instead of `SELECT *`
   - Implement data sampling for large files

### Debugging

Enable debug logging by setting:
```python
logging.basicConfig(level=logging.DEBUG)
```

## Integration Examples

### With Jupyter Notebooks
```python
import requests
import pandas as pd

# Query data through API
response = requests.post("http://localhost:8002/query/parquet", params={
    "bucket_name": "data-lake",
    "file_path": "processed/data.parquet",
    "query": "SELECT * FROM {table} WHERE category = 'sales'",
    "limit": 1000
})

if response.status_code == 200:
    data = response.json()["data"]
    df = pd.DataFrame(data)
    print(df.head())
```

### With Other Services
The REST API can be integrated with:
- Business Intelligence tools
- Data visualization platforms
- Automated reporting systems
- ETL pipelines
- Machine learning workflows

## Security

- Configure proper MinIO access credentials
- Use environment variables for sensitive configuration
- Implement authentication for production deployments
- Set appropriate query limits to prevent resource exhaustion
- Consider network security and access controls

## Future Enhancements

- Query result caching
- User authentication and authorization
- Query history and favorites
- Advanced visualization capabilities
- Real-time data refresh
- Query performance monitoring
- Support for multiple MinIO instances
