# Big Data & Data Lakehouse Final Project
# Hospital Cost and Duration Prediction System

## Overview
This project implements a big data system for predicting hospital costs and length of stay using the 2015 De-identified NY Inpatient Discharge SPARCS dataset from Kaggle.

## Architecture
- **Kafka**: Data streaming and message queuing
- **MinIO**: S3-compatible object storage for data lake
- **Docker**: Containerized deployment
- **KaggleHub**: Automatic dataset download
- **Data Lakehouse**: Storage and processing architecture

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- Kaggle API credentials
- uv package manager

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

3. **Setup**
   ```bash
   ./setup.sh
   ```

   or,

   ```
   ./setup.bat
   ```

4. **Run the System**
   ```bash
   ./manage.sh [ARGS]
   ```

   or,

   ```
   ./manage.bat [ARGS]
   ```

## Services

### Kafka Cluster
- **Zookeeper**: `localhost:2181`
- **Kafka Broker**: `localhost:9092`
- **Kafka UI**: `http://localhost:8080`

### MinIO Object Storage
- **MinIO Console**: `http://localhost:9090`
- **MinIO API**: `localhost:9000`
- **Default Credentials**: `minioadmin` / `minioadmin`

### Data Pipeline
- **Data Producer**: Downloads dataset and streams to Kafka
- **Data Consumer**: Consumes from Kafka and stores to MinIO
- **Storage Formats**: JSON (raw) and Parquet (optimized)

## Monitoring
- **Kafka UI**: `http://localhost:8080` - Monitor topics and messages
- **MinIO Console**: `http://localhost:9090` - Monitor object storage
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
