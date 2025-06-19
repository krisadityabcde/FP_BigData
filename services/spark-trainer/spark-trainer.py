import os
import json
import time
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, regexp_extract, regexp_replace, trim, upper
from pyspark.sql.types import FloatType, IntegerType, StringType, StructType, StructField
from pyspark.ml.feature import VectorAssembler, Imputer, StringIndexer, OneHotEncoder, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from minio import Minio
from minio.error import S3Error
import pickle
from io import BytesIO

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HospitalMLTrainer:
    def __init__(self):
        # Spark configuration
        self.spark = None
        
        # MinIO configuration for data and model storage
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.bucket_name = os.getenv('MINIO_BUCKET', 'hospital-data')
        self.model_bucket = os.getenv('MINIO_MODEL_BUCKET', 'hospital-models')
        
        self.minio_client = None
        
        # Model configuration
        self.target_columns = ['Length of Stay', 'Total Costs', 'Total Charges']
        self.models = {}
        
    def initialize_spark(self):
        """Initialize Spark session with optimized configuration"""
        try:
            self.spark = SparkSession.builder \
                .appName("HospitalMLTrainer") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .getOrCreate()
            
            logger.info("Spark session initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {e}")
            return False
    
    def initialize_minio_client(self):
        """Initialize MinIO client"""
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.minio_client = Minio(
                    self.minio_endpoint,
                    access_key=self.minio_access_key,
                    secret_key=self.minio_secret_key,
                    secure=False
                )
                
                # Test connection
                self.minio_client.list_buckets()
                logger.info(f"Successfully connected to MinIO at {self.minio_endpoint}")
                
                # Create model bucket if it doesn't exist
                if not self.minio_client.bucket_exists(self.model_bucket):
                    self.minio_client.make_bucket(self.model_bucket)
                    logger.info(f"Created model bucket: {self.model_bucket}")
                
                return True
            except Exception as e:
                retry_count += 1
                logger.warning(f"Failed to connect to MinIO (attempt {retry_count}/{max_retries}): {e}")
                time.sleep(10)
        
        logger.error("Failed to connect to MinIO after maximum retries")
        return False
    
    def wait_for_data(self, min_files=3, max_wait_time=600):
        """Wait for sufficient data to be available in MinIO"""
        logger.info(f"Waiting for at least {min_files} data files to be available...")
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                if not self.minio_client.bucket_exists(self.bucket_name):
                    logger.info(f"Data bucket {self.bucket_name} doesn't exist yet, waiting...")
                    time.sleep(30)
                    continue
                
                objects = list(self.minio_client.list_objects(self.bucket_name, recursive=True))
                data_files = [obj for obj in objects if obj.object_name.endswith(('.json', '.csv')) 
                             and not obj.object_name.endswith('metadata.json')]
                
                logger.info(f"Found {len(data_files)} data files in MinIO")
                
                if len(data_files) >= min_files:
                    logger.info(f"Sufficient data available ({len(data_files)} files)")
                    return True
                
                logger.info(f"Waiting for more data... ({len(data_files)}/{min_files} files)")
                time.sleep(60)  # Wait 1 minute before checking again
                
            except Exception as e:
                logger.warning(f"Error checking data availability: {e}")
                time.sleep(30)
        
        logger.warning(f"Timeout waiting for data after {max_wait_time} seconds")
        return False
    
    def load_data_from_minio(self):
        """Load training data from MinIO"""
        try:
            logger.info("Loading data from MinIO...")
            
            # Check if data bucket exists
            if not self.minio_client.bucket_exists(self.bucket_name):
                logger.error(f"Data bucket {self.bucket_name} does not exist")
                return None
            
            # List all files in the bucket
            objects = list(self.minio_client.list_objects(self.bucket_name, recursive=True))
            logger.info(f"Found {len(objects)} total objects in bucket {self.bucket_name}")
            
            # Filter for data files
            csv_files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]
            json_files = [obj.object_name for obj in objects if obj.object_name.endswith('.json') 
                         and not obj.object_name.endswith('metadata.json')]
            
            logger.info(f"Found {len(csv_files)} CSV files and {len(json_files)} JSON files")
            
            if not csv_files and not json_files:
                logger.error("No data files found in MinIO")
                logger.info("Available objects:")
                for obj in objects[:10]:  # Show first 10 objects
                    logger.info(f"  - {obj.object_name}")
                return None
            
            # Prefer JSON files for simplicity, fallback to CSV
            if json_files:
                logger.info("Loading JSON files (more reliable for this setup)...")
                return self.load_json_data(json_files[:10])  # Use more JSON files
            elif csv_files:
                logger.info("Loading CSV files...")
                return self.load_csv_data(csv_files)
            else:
                logger.error("No suitable data files found")
                return None
            
        except Exception as e:
            logger.error(f"Error loading data from MinIO: {e}")
            return None
    
    def load_csv_data(self, csv_files):
        """Load data from CSV files"""
        try:
            # Create a persistent temp directory for this session
            import tempfile
            temp_dir = tempfile.mkdtemp(prefix="spark_trainer_")
            logger.info(f"Using temporary directory: {temp_dir}")
            
            dfs = []
            temp_files = []
            
            for file_path in csv_files[:5]:  # Limit to first 5 files for training
                try:
                    # Get file from MinIO
                    response = self.minio_client.get_object(self.bucket_name, file_path)
                    
                    # Save to persistent temp location
                    safe_filename = file_path.replace('/', '_').replace('=', '_')
                    temp_path = os.path.join(temp_dir, safe_filename)
                    
                    with open(temp_path, 'wb') as f:
                        f.write(response.read())
                    
                    temp_files.append(temp_path)
                    logger.info(f"Downloaded {file_path} to {temp_path}")
                    
                except Exception as e:
                    logger.warning(f"Could not download {file_path}: {e}")
                    continue
            
            if not temp_files:
                logger.error("No CSV files could be downloaded")
                return None
            
            # Read all files and collect data immediately to avoid lazy evaluation issues
            logger.info(f"Reading {len(temp_files)} CSV files with Spark...")
            all_data = []
            
            for temp_path in temp_files:
                try:
                    # Read CSV with Spark
                    df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(temp_path)
                    # Force execution and collect schema info
                    row_count = df.count()
                    logger.info(f"File {os.path.basename(temp_path)}: {row_count} rows")
                    
                    if row_count > 0:
                        # Convert to pandas and back to ensure data is materialized
                        pandas_df = df.toPandas()
                        spark_df = self.spark.createDataFrame(pandas_df)
                        dfs.append(spark_df)
                    
                except Exception as e:
                    logger.warning(f"Could not read CSV file {temp_path}: {e}")
                    continue
            
            # Clean up temp files
            try:
                import shutil
                shutil.rmtree(temp_dir)
                logger.info("Cleaned up temporary files")
            except Exception as e:
                logger.warning(f"Could not clean up temp directory: {e}")
            
            if not dfs:
                logger.error("No valid CSV data could be loaded")
                return None
            
            # Union all dataframes
            logger.info(f"Combining {len(dfs)} dataframes...")
            combined_df = dfs[0]
            for df in dfs[1:]:
                combined_df = combined_df.union(df)
            
            total_records = combined_df.count()
            logger.info(f"Successfully loaded {total_records} records from {len(dfs)} CSV files")
            return combined_df
            return combined_df
            
        except Exception as e:
            logger.error(f"Error loading parquet data: {e}")
            return None
    
    def load_json_data(self, json_files):
        """Load data from JSON files as primary method"""
        try:
            all_data = []
            files_processed = 0
            max_records = 10000  # Limit total records for training
            
            for file_path in json_files:
                if len(all_data) >= max_records:
                    logger.info(f"Reached maximum records limit ({max_records}), stopping data loading")
                    break
                    
                try:
                    logger.info(f"Loading JSON file: {file_path}")
                    response = self.minio_client.get_object(self.bucket_name, file_path)
                    data = json.loads(response.read().decode('utf-8'))
                    
                    if isinstance(data, list):
                        all_data.extend(data)
                        logger.info(f"  Added {len(data)} records from {file_path}")
                    else:
                        all_data.append(data)
                        logger.info(f"  Added 1 record from {file_path}")
                    
                    files_processed += 1
                    
                except Exception as e:
                    logger.warning(f"Could not load {file_path}: {e}")
                    continue
            
            if not all_data:
                logger.error("No data could be loaded from JSON files")
                return None
            
            logger.info(f"Total records loaded: {len(all_data)} from {files_processed} JSON files")
            
            # Convert to Spark DataFrame with better error handling
            try:
                df = self.spark.createDataFrame(all_data)
                record_count = df.count()
                logger.info(f"Successfully created Spark DataFrame with {record_count} records")
                return df
            except Exception as e:
                logger.error(f"Failed to create Spark DataFrame: {e}")
                logger.info("Attempting to create DataFrame with schema inference...")
                
                # Try with a smaller sample first to infer schema
                sample_data = all_data[:100] if len(all_data) > 100 else all_data
                try:
                    sample_df = self.spark.createDataFrame(sample_data)
                    schema = sample_df.schema
                    df = self.spark.createDataFrame(all_data, schema)
                    logger.info(f"Successfully created DataFrame with inferred schema")
                    return df
                except Exception as e2:
                    logger.error(f"Schema inference also failed: {e2}")
                    return None
            
        except Exception as e:
            logger.error(f"Error loading JSON data: {e}")
            return None
    
    def preprocess_data(self, df):
        """Preprocess the hospital data for ML training"""
        try:
            logger.info("Starting data preprocessing...")
            
            # Show data schema and sample
            logger.info("Data schema:")
            df.printSchema()
            
            # Clean column names (remove special characters, spaces)
            clean_columns = []
            for col_name in df.columns:
                clean_name = col_name.replace(' ', '_').replace('-', '_').replace('.', '_')
                clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
                clean_columns.append(clean_name)
            
            # Rename columns
            for old_name, new_name in zip(df.columns, clean_columns):
                df = df.withColumnRenamed(old_name, new_name)
            
            # Update target column names
            target_mapping = {
                'Length_of_Stay': 'Length of Stay',
                'Total_Costs': 'Total Costs', 
                'Total_Charges': 'Total Charges'
            }
            
            # Convert target columns to numeric
            numeric_targets = []
            for old_col, original_col in target_mapping.items():
                if old_col in df.columns:
                    # Clean and convert to numeric
                    df = df.withColumn(f"{old_col}_numeric", 
                        regexp_replace(col(old_col).cast("string"), "[^0-9.]", "").cast("double"))
                    numeric_targets.append(f"{old_col}_numeric")
            
            # Select relevant features for modeling
            feature_columns = []
            categorical_columns = []
            
            for col_name in df.columns:
                if col_name in ['Age_Group', 'Gender', 'Race', 'Ethnicity', 'Type_of_Admission',
                               'Patient_Disposition', 'CCS_Diagnosis_Description', 'CCS_Procedure_Description',
                               'APR_DRG_Description', 'APR_Medical_Surgical_Description',
                               'APR_Severity_of_Illness_Description', 'APR_Risk_of_Mortality',
                               'Hospital_County', 'Operating_Certificate_Number']:
                    categorical_columns.append(col_name)
                elif col_name in ['Birth_Weight', 'APR_DRG_Code', 'APR_Severity_of_Illness_Code']:
                    # Try to convert to numeric
                    df = df.withColumn(f"{col_name}_numeric", col(col_name).cast("double"))
                    feature_columns.append(f"{col_name}_numeric")
            
            # Handle missing values in categorical columns
            for col_name in categorical_columns:
                df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
            
            # Index categorical features
            indexers = []
            encoders = []
            indexed_cols = []
            encoded_cols = []
            
            for col_name in categorical_columns:
                # Use handleInvalid="keep" to assign unknown categorical values to a special index
                indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_indexed", handleInvalid="keep")
                encoder = OneHotEncoder(inputCol=f"{col_name}_indexed", outputCol=f"{col_name}_encoded", handleInvalid="keep")
                
                indexers.append(indexer)
                encoders.append(encoder)
                indexed_cols.append(f"{col_name}_indexed")
                encoded_cols.append(f"{col_name}_encoded")
            
            # Create feature vector
            all_feature_cols = feature_columns + encoded_cols
            assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features", handleInvalid="keep")
            
            # Scale features
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
            
            logger.info(f"Preprocessing pipeline created with {len(all_feature_cols)} features")
            logger.info(f"Categorical columns: {categorical_columns}")
            logger.info(f"Numeric columns: {feature_columns}")
            logger.info(f"Target columns: {numeric_targets}")
            
            return df, indexers, encoders, assembler, scaler, numeric_targets, all_feature_cols
            
        except Exception as e:
            logger.error(f"Error in data preprocessing: {e}")
            return None, None, None, None, None, None, None
    
    def train_models(self, df, indexers, encoders, assembler, scaler, target_cols, feature_cols):
        """Train ML models for each target variable"""
        try:
            logger.info("Starting model training...")
            
            # Build preprocessing pipeline
            pipeline_stages = indexers + encoders + [assembler, scaler]
            
            # Train models for each target
            for target_col in target_cols:
                if target_col not in df.columns:
                    logger.warning(f"Target column {target_col} not found, skipping...")
                    continue
                
                logger.info(f"Training model for {target_col}...")
                
                # Filter out null values in target
                target_df = df.filter(col(target_col).isNotNull() & (col(target_col) > 0))
                
                if target_df.count() < 100:
                    logger.warning(f"Insufficient data for {target_col} (only {target_df.count()} records)")
                    continue
                
                # Split data
                train_df, test_df = target_df.randomSplit([0.8, 0.2], seed=42)
                
                # Create model pipelines
                models_to_train = [
                    ("RandomForest", RandomForestRegressor(
                        featuresCol="scaled_features", 
                        labelCol=target_col,
                        numTrees=50,
                        maxDepth=10
                    )),
                    ("GBT", GBTRegressor(
                        featuresCol="scaled_features",
                        labelCol=target_col,
                        maxIter=50,
                        maxDepth=8
                    ))
                ]
                
                best_model = None
                best_rmse = float('inf')
                best_model_name = None
                
                for model_name, model in models_to_train:
                    try:
                        # Create full pipeline
                        full_pipeline = Pipeline(stages=pipeline_stages + [model])
                        
                        # Train model
                        trained_model = full_pipeline.fit(train_df)
                        
                        # Evaluate on test set
                        predictions = trained_model.transform(test_df)
                        evaluator = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="rmse")
                        rmse = evaluator.evaluate(predictions)
                        
                        logger.info(f"{model_name} for {target_col} - RMSE: {rmse:.2f}")
                        
                        if rmse < best_rmse:
                            best_rmse = rmse
                            best_model = trained_model
                            best_model_name = model_name
                            
                    except Exception as e:
                        logger.warning(f"Failed to train {model_name} for {target_col}: {e}")
                        continue
                
                if best_model:
                    self.models[target_col] = {
                        'model': best_model,
                        'rmse': best_rmse,
                        'model_type': best_model_name,
                        'feature_columns': feature_cols
                    }
                    logger.info(f"Best model for {target_col}: {best_model_name} (RMSE: {best_rmse:.2f})")
                else:
                    logger.warning(f"No successful model trained for {target_col}")
            
            logger.info(f"Model training completed. Trained {len(self.models)} models.")
            return True
            
        except Exception as e:
            logger.error(f"Error in model training: {e}")
            return False
    
    def save_models_to_minio(self):
        """Save trained models to MinIO"""
        try:
            logger.info("Saving models to MinIO...")
            
            for target_col, model_info in self.models.items():
                try:
                    # Save Spark ML model
                    model_name = f"{target_col.replace(' ', '_').lower()}_model"
                    temp_path = f"/tmp/{model_name}"
                    
                    # Save model to temporary directory
                    model_info['model'].write().overwrite().save(temp_path)
                    
                    # Create tar archive of model directory
                    import tarfile
                    tar_path = f"/tmp/{model_name}.tar.gz"
                    with tarfile.open(tar_path, "w:gz") as tar:
                        tar.add(temp_path, arcname=model_name)
                    
                    # Upload to MinIO
                    with open(tar_path, 'rb') as file_data:
                        self.minio_client.put_object(
                            self.model_bucket,
                            f"models/{model_name}.tar.gz",
                            file_data,
                            os.path.getsize(tar_path)
                        )
                    
                    # Save model metadata
                    metadata = {
                        'target_column': target_col,
                        'model_type': model_info['model_type'],
                        'rmse': model_info['rmse'],
                        'feature_columns': model_info['feature_columns'],
                        'training_timestamp': datetime.now().isoformat(),
                        'model_path': f"models/{model_name}.tar.gz"
                    }
                    
                    metadata_json = json.dumps(metadata, indent=2)
                    metadata_bytes = BytesIO(metadata_json.encode('utf-8'))
                    
                    self.minio_client.put_object(
                        self.model_bucket,
                        f"metadata/{model_name}_metadata.json",
                        metadata_bytes,
                        len(metadata_json.encode('utf-8')),
                        content_type='application/json'
                    )
                    
                    # Clean up temporary files
                    os.system(f"rm -rf {temp_path}")
                    os.remove(tar_path)
                    
                    logger.info(f"Saved model for {target_col} to MinIO")
                    
                except Exception as e:
                    logger.error(f"Error saving model for {target_col}: {e}")
                    continue
            
            logger.info("Model saving completed")
            return True
            
        except Exception as e:
            logger.error(f"Error saving models to MinIO: {e}")
            return False
    
    def run(self):
        """Main execution method"""
        logger.info("Starting Hospital ML Trainer...")
        
        # Wait for services to be ready
        logger.info("Waiting for services to be ready...")
        time.sleep(60)
        
        # Initialize components
        if not self.initialize_spark():
            return False
        
        if not self.initialize_minio_client():
            return False
        
        # Wait for data to be available
        if not self.wait_for_data():
            logger.error("No sufficient data available for training")
            return False
        
        try:
            # Wait for data to be available
            if not self.wait_for_data():
                logger.warning("No sufficient data available, trying to proceed with available data...")
            
            # Load data
            df = self.load_data_from_minio()
            if df is None:
                logger.error("Failed to load training data")
                return False
            
            # Verify data is accessible
            try:
                record_count = df.count()
                column_count = len(df.columns)
                logger.info(f"Data verification: {record_count} records, {column_count} columns")
                
                if record_count < 50:
                    logger.warning(f"Very few records available ({record_count}), training may not be effective")
                
                # Show sample of data
                logger.info("Sample of loaded data:")
                df.show(5, truncate=False)
                
            except Exception as e:
                logger.error(f"Data verification failed: {e}")
                return False
            
            # Preprocess data
            preprocessed_data = self.preprocess_data(df)
            if preprocessed_data[0] is None:
                logger.error("Failed to preprocess data")
                return False
            
            df, indexers, encoders, assembler, scaler, target_cols, feature_cols = preprocessed_data
            
            # Train models
            if not self.train_models(df, indexers, encoders, assembler, scaler, target_cols, feature_cols):
                logger.error("Failed to train models")
                return False
            
            # Save models
            if not self.save_models_to_minio():
                logger.error("Failed to save models")
                return False
            
            logger.info("ML training pipeline completed successfully")
            return True
            
        finally:
            if self.spark:
                self.spark.stop()

if __name__ == "__main__":
    import sys
    
    trainer = HospitalMLTrainer()
    
    # Check if running in continuous mode
    continuous_mode = len(sys.argv) > 1 and sys.argv[1] == '--continuous'
    
    if continuous_mode:
        logger.info("Running in continuous training mode...")
        while True:
            try:
                success = trainer.run()
                if success:
                    logger.info("Training completed successfully. Waiting 1 hour before next training...")
                    time.sleep(3600)  # Wait 1 hour
                else:
                    logger.warning("Training failed. Waiting 30 minutes before retry...")
                    time.sleep(1800)  # Wait 30 minutes
            except KeyboardInterrupt:
                logger.info("Continuous training stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in continuous mode: {e}")
                time.sleep(1800)  # Wait 30 minutes before retry
    else:
        # Single run mode
        trainer.run()