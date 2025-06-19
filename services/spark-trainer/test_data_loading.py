#!/usr/bin/env python3
"""
Quick test script for Spark Trainer data loading
Tests data loading functionality without full training
"""

import os
import sys
import logging
import time

# Add the spark-trainer directory to the path
sys.path.append('/app')

# Import after path modification
try:
    from spark_trainer import HospitalMLTrainer
except ImportError:
    # Fallback for local testing
    import importlib.util
    spec = importlib.util.spec_from_file_location("spark_trainer", "/app/spark-trainer.py")
    spark_trainer_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(spark_trainer_module)
    HospitalMLTrainer = spark_trainer_module.HospitalMLTrainer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_data_loading():
    """Test just the data loading part"""
    logger.info("=== Testing Spark Trainer Data Loading ===")
    
    trainer = HospitalMLTrainer()
    
    # Initialize components
    logger.info("1. Initializing Spark...")
    if not trainer.initialize_spark():
        logger.error("Failed to initialize Spark")
        return False
    
    logger.info("2. Initializing MinIO...")
    if not trainer.initialize_minio_client():
        logger.error("Failed to initialize MinIO")
        return False
    
    logger.info("3. Checking data availability...")
    try:
        # Check bucket exists
        if not trainer.minio_client.bucket_exists(trainer.bucket_name):
            logger.error(f"Bucket {trainer.bucket_name} does not exist")
            return False
        
        # List files
        objects = list(trainer.minio_client.list_objects(trainer.bucket_name, recursive=True))
        json_files = [obj.object_name for obj in objects if obj.object_name.endswith('.json') 
                     and not obj.object_name.endswith('metadata.json')]
        parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
        
        logger.info(f"Found {len(objects)} total objects")
        logger.info(f"Found {len(json_files)} JSON files")
        logger.info(f"Found {len(parquet_files)} parquet files")
        
        if len(json_files) < 3 and len(parquet_files) < 3:
            logger.warning("Insufficient data files for testing")
            return False
            
    except Exception as e:
        logger.error(f"Error checking data: {e}")
        return False
    
    logger.info("4. Testing data loading...")
    try:
        df = trainer.load_data_from_minio()
        if df is None:
            logger.error("Data loading returned None")
            return False
        
        # Test basic operations
        record_count = df.count()
        column_count = len(df.columns)
        
        logger.info(f"✅ Successfully loaded {record_count} records with {column_count} columns")
        
        # Show schema
        logger.info("Data schema:")
        df.printSchema()
        
        # Show sample
        logger.info("Sample data:")
        df.show(3, truncate=True)
        
        return True
        
    except Exception as e:
        logger.error(f"Data loading test failed: {e}")
        return False
    
    finally:
        if trainer.spark:
            trainer.spark.stop()

if __name__ == "__main__":
    success = test_data_loading()
    if success:
        logger.info("🎉 Data loading test PASSED")
        sys.exit(0)
    else:
        logger.error("❌ Data loading test FAILED")
        sys.exit(1)
