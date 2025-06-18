# Big Data & Data Lakehouse Final Project
# Hospital Cost and Duration Prediction System

## Overview
This project implements a comprehensive big data system for analyzing hospital costs and length of stay using the 2015 De-identified NY Inpatient Discharge SPARCS dataset from Kaggle. The system features real-time data streaming, object storage, and interactive analytics capabilities.

## Architecture
- **Kafka**: Real-time data streaming and message queuing
- **MinIO**: S3-compatible object storage for data lake
- **DuckDB**: High-performance analytics engine for interactive queries
- **Streamlit**: Interactive web interface for data exploration
- **FastAPI**: REST API for programmatic data access
- **Docker**: Containerized deployment and orchestration

## System Components

### 1. Data Ingestion Layer
- **Kafka Producer**: Streams data from Kaggle to Kafka topics
- **Kafka Consumer**: Processes stream data and stores as Parquet files

### 2. Storage Layer  
- **MinIO**: Object storage for Parquet files (data lake)
- **Kafka Topics**: Message streaming and buffering

### 3. Analytics Layer
- **DuckDB Query Engine**: SQL analytics on Parquet files
- **Streamlit Interface**: Interactive web-based analytics
- **FastAPI Service**: REST API for query execution

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- Kaggle API credentials
- Minimum 4GB RAM, 10GB storage

### Setup Instructions

1. **Get Kaggle API Credentials**
   - Go to https://www.kaggle.com/settings/account
   - Click "Create New API Token" to download `kaggle.json`
   - Note down your username and key from the file

2. **Configure Environment**
   ```bash
   cp .env.example .env
   # Edit .env file with your Kaggle credentials
   ```

3. **Run the System**
   ```bash
   docker-compose up --build
   ```

4. **Access Services**
   - **Analytics Interface**: http://localhost:8501 (Streamlit)
   - **Query API**: http://localhost:8002 (FastAPI)
   - **MinIO Console**: http://localhost:9090
   - **Kafka UI**: http://localhost:8080

## Services & Endpoints

### Analytics Services
- **DuckDB Query Interface**: `http://localhost:8501`
  - Interactive SQL query builder
  - Data visualization and exploration
  - Schema inspection and analysis
  - Export capabilities

- **Query API**: `http://localhost:8002`
  - REST endpoints for programmatic access
  - Health checks and monitoring
  - Direct Parquet file querying

### Infrastructure Services
- **Kafka Cluster**
  - Zookeeper: `localhost:2181`
  - Kafka Broker: `localhost:9092`
  - Kafka UI: `http://localhost:8080`

- **MinIO Object Storage**
  - Console: `http://localhost:9090`
  - API: `localhost:9000`
  - Default Credentials: `minioadmin` / `minioadmin`

### Data Pipeline
- **Data Producer**: Downloads dataset and streams to Kafka
- **Data Consumer**: Consumes from Kafka and stores to MinIO as Parquet
- **Query Engine**: DuckDB for direct analytics on Parquet files
- **Storage Formats**: JSON (streaming) and Parquet (analytics-optimized)

## Sample Analytics Queries

Access the Streamlit interface at `http://localhost:8501` to run these queries:

### Basic Data Exploration
```sql
-- Get total number of records
SELECT COUNT(*) as total_records FROM {table};

-- View sample data
SELECT * FROM {table} LIMIT 10;

-- Get unique hospitals
SELECT DISTINCT hospital_county, COUNT(*) as patient_count 
FROM {table} 
GROUP BY hospital_county 
ORDER BY patient_count DESC;
```

### Cost Analysis
```sql
-- Average cost by diagnosis
SELECT 
    diagnosis_code,
    COUNT(*) as case_count,
    AVG(total_charges) as avg_cost,
    AVG(length_of_stay) as avg_stay
FROM {table}
GROUP BY diagnosis_code
ORDER BY avg_cost DESC
LIMIT 20;
```

### Temporal Analysis
```sql
-- Monthly admission trends
SELECT 
    DATE_TRUNC('month', admission_date) as month,
    COUNT(*) as admissions,
    AVG(total_charges) as avg_charges
FROM {table}
WHERE admission_date >= '2015-01-01'
GROUP BY DATE_TRUNC('month', admission_date)
ORDER BY month;
```

## API Usage Examples

### Query Parquet Files via REST API
```bash
# Basic query
curl -X POST "http://localhost:8002/query/parquet" \
  -H "Content-Type: application/json" \
  -d '{
    "bucket_name": "hospital-data",
    "file_path": "processed/hospital_data.parquet",
    "query": "SELECT * FROM {table} WHERE total_charges > 50000 LIMIT 100"
  }'

# Get schema information
curl "http://localhost:8002/schema/hospital-data?file_path=processed/hospital_data.parquet"
```

## Monitoring & Management
- **Kafka UI**: `http://localhost:8080` - Monitor topics and messages
- **MinIO Console**: `http://localhost:9090` - Monitor object storage
- **Query API Health**: `http://localhost:8002/health` - Service health check
- **Logs**: `docker-compose logs -f [service-name]`
- **Storage Monitor**: `docker exec -it data-consumer python monitor.py`

## Project Structure
```
FP_BigData/
├── docker-compose.yml          # Main orchestration file
├── services/
│   ├── data-producer/         # Kafka producer service
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── producer.py
│   └── data-consumer/         # Kafka to MinIO consumer
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── consumer.py
│       └── monitor.py
├── data/                      # Dataset storage
└── README.md
```

## Dataset Information
- **Source**: 2015 De-identified NY Inpatient Discharge SPARCS
- **URL**: https://www.kaggle.com/datasets/jonasalmeida/2015-deidentified-ny-inpatient-discharge-sparcs
- **Features**: Patient demographics, diagnoses, procedures, costs, length of stay
- **Size**: Large dataset requiring big data processing techniques

## Next Steps
This is the foundation setup. The system will be extended with:
- Data processing components (Spark/Hadoop)
- Machine learning pipeline
- Data lakehouse storage (Delta Lake)
- Prediction API
- Visualization dashboard

## Development
To add new services or modify existing ones, follow the Docker Compose pattern and add services to the `docker-compose.yml` file.
