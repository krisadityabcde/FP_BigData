# Hospital Prediction Services

This directory contains two main services for the Hospital Big Data Pipeline:

## 1. Spark Trainer Service (`spark-trainer/`)

The Spark Trainer service is responsible for training machine learning models using Apache Spark on the hospital discharge data.

### Features:
- **Data Loading**: Loads data from MinIO storage (both JSON and Parquet formats)
- **Data Preprocessing**: Cleans and transforms hospital data for ML training
- **Model Training**: Trains multiple ML models (Random Forest, Gradient Boosting) for different targets:
  - Length of Stay prediction
  - Total Costs prediction  
  - Total Charges prediction
- **Model Evaluation**: Uses cross-validation and RMSE metrics
- **Model Storage**: Saves trained models to MinIO for later use

### Key Files:
- `spark-trainer.py`: Main training script
- `monitor.py`: Monitoring script to check training progress
- `requirements.txt`: Python dependencies
- `Dockerfile`: Container configuration

### Environment Variables:
- `MINIO_ENDPOINT`: MinIO server endpoint (default: localhost:9000)
- `MINIO_ACCESS_KEY`: MinIO access key (default: minioadmin)
- `MINIO_SECRET_KEY`: MinIO secret key (default: minioadmin)
- `MINIO_BUCKET`: Data bucket name (default: hospital-data)
- `MINIO_MODEL_BUCKET`: Model storage bucket (default: hospital-models)

## 2. API Service (`api/`)

The API service provides REST endpoints for making predictions using the trained models.

### Features:
- **Model Loading**: Automatically loads trained models from MinIO
- **Real-time Predictions**: Provides REST API for real-time predictions
- **Multiple Models**: Supports predictions from all available trained models
- **Web Interface**: Includes documentation and testing interface
- **Health Monitoring**: Health check and model status endpoints

### API Endpoints:

#### GET `/`
Returns API documentation webpage

#### GET `/health`
Health check endpoint
```json
{
  "status": "healthy",
  "models_loaded": 3,
  "timestamp": "2025-06-18T10:30:00"
}
```

#### GET `/models`
List all available models and their metadata

#### POST `/predict/<model_name>`
Make prediction using specific model
```json
{
  "age_group": "30 to 49",
  "gender": "M",
  "race": "White",
  "ethnicity": "Not Span/Hispanic",
  "admission_type": "Emergency",
  "disposition": "Home or Self Care",
  "diagnosis": "Mood disorders",
  "procedure": "No procedure",
  "severity": "Minor",
  "mortality_risk": "Minor",
  "county": "New York",
  "birth_weight": 0,
  "drg_code": 444,
  "severity_code": 1
}
```

#### POST `/predict/all`
Make predictions using all available models

#### POST `/reload-models`
Reload models from MinIO storage

### Key Files:
- `api.py`: Main Flask application
- `test_api.py`: Comprehensive API testing script
- `requirements.txt`: Python dependencies
- `Dockerfile`: Container configuration

### Testing:
```bash
# Test API locally
python test_api.py

# Test API on different host
python test_api.py http://api-service:5000
```

## Data Pipeline Flow

1. **Data Producer** → Streams hospital data to Kafka
2. **Data Consumer** → Consumes from Kafka and stores in MinIO
3. **Spark Trainer** → Loads data from MinIO, trains models, saves models back to MinIO
4. **API Service** → Loads models from MinIO and serves predictions

## Input Data Format

The services expect hospital discharge data with the following key fields:

### Categorical Features:
- `Age Group`: Patient age group (e.g., "30 to 49")
- `Gender`: Patient gender (M/F)
- `Race`: Patient race
- `Ethnicity`: Patient ethnicity
- `Type of Admission`: Emergency, Elective, etc.
- `Patient Disposition`: Discharge disposition
- `CCS Diagnosis Description`: Clinical diagnosis
- `CCS Procedure Description`: Medical procedure
- `APR DRG Description`: Diagnosis Related Group
- `APR Severity of Illness Description`: Illness severity
- `APR Risk of Mortality`: Mortality risk level
- `Hospital County`: Hospital location

### Numeric Features:
- `Birth Weight`: Patient birth weight
- `APR DRG Code`: Numeric DRG code
- `APR Severity of Illness Code`: Numeric severity code

### Target Variables:
- `Length of Stay`: Hospital stay duration (days)
- `Total Costs`: Total medical costs ($)
- `Total Charges`: Total charges ($)

## Model Performance

The trained models provide predictions for:
- **Length of Stay**: Predicts how many days a patient will stay
- **Total Costs**: Predicts the total medical costs
- **Total Charges**: Predicts the total charges

Models are evaluated using RMSE (Root Mean Square Error) and the best performing model type is automatically selected for each target variable.

## Monitoring

Use the monitoring scripts to check system status:

```bash
# Check Spark trainer status
python services/spark-trainer/monitor.py --once

# Continuous monitoring
python services/spark-trainer/monitor.py

# Test API functionality
python services/api/test_api.py
```

## Deployment

Both services are containerized and can be deployed using Docker Compose:

```bash
# Build and start services
docker-compose up --build

# Scale services
docker-compose up --scale spark-trainer=2 --scale api=3
```

The services automatically connect to Kafka and MinIO instances and will retry connections until successful.
