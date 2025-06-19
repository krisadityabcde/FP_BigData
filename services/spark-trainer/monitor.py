#!/usr/bin/env python3
"""
Monitor script for Spark ML Trainer
Checks training progress and model availability
"""

import os
import time
import logging
import json
from datetime import datetime
from minio import Minio
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SparkTrainerMonitor:
    def __init__(self):
        # MinIO configuration
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.bucket_name = os.getenv('MINIO_BUCKET', 'hospital-data')
        self.model_bucket = os.getenv('MINIO_MODEL_BUCKET', 'hospital-models')
        
        self.minio_client = None
        
    def initialize_minio_client(self):
        """Initialize MinIO client"""
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
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MinIO: {e}")
            return False
    
    def check_training_data(self):
        """Check if training data is available"""
        try:
            if not self.minio_client.bucket_exists(self.bucket_name):
                logger.warning(f"Data bucket {self.bucket_name} does not exist")
                return False
            
            objects = list(self.minio_client.list_objects(self.bucket_name, recursive=True))
            data_files = [obj for obj in objects if obj.object_name.endswith(('.json', '.csv'))]
            
            logger.info(f"Found {len(data_files)} data files in bucket {self.bucket_name}")
            
            if data_files:
                # Show some sample files
                for i, obj in enumerate(data_files[:5]):
                    logger.info(f"  Data file {i+1}: {obj.object_name} ({obj.size} bytes)")
                
                return True
            else:
                logger.warning("No data files found for training")
                return False
                
        except Exception as e:
            logger.error(f"Error checking training data: {e}")
            return False
    
    def check_trained_models(self):
        """Check if trained models are available"""
        try:
            if not self.minio_client.bucket_exists(self.model_bucket):
                logger.info(f"Model bucket {self.model_bucket} does not exist yet")
                return False
            
            objects = list(self.minio_client.list_objects(self.model_bucket, recursive=True))
            model_files = [obj for obj in objects if obj.object_name.endswith('.tar.gz')]
            metadata_files = [obj for obj in objects if obj.object_name.endswith('_metadata.json')]
            
            logger.info(f"Found {len(model_files)} trained models")
            logger.info(f"Found {len(metadata_files)} metadata files")
            
            # Display model information
            for metadata_file in metadata_files:
                try:
                    response = self.minio_client.get_object(self.model_bucket, metadata_file.object_name)
                    metadata = json.loads(response.read().decode('utf-8'))
                    
                    logger.info(f"Model: {metadata.get('target_column', 'Unknown')}")
                    logger.info(f"  Type: {metadata.get('model_type', 'Unknown')}")
                    logger.info(f"  RMSE: {metadata.get('rmse', 'Unknown')}")
                    logger.info(f"  Training time: {metadata.get('training_timestamp', 'Unknown')}")
                    logger.info(f"  Features: {len(metadata.get('feature_columns', []))}")
                    
                except Exception as e:
                    logger.warning(f"Could not read metadata from {metadata_file.object_name}: {e}")
            
            return len(model_files) > 0
            
        except Exception as e:
            logger.error(f"Error checking trained models: {e}")
            return False
    
    def get_system_status(self):
        """Get overall system status"""
        status = {
            'timestamp': datetime.now().isoformat(),
            'minio_connected': False,
            'training_data_available': False,
            'models_available': False,
            'data_file_count': 0,
            'model_count': 0
        }
        
        # Check MinIO connection
        if self.initialize_minio_client():
            status['minio_connected'] = True
            
            # Check training data
            status['training_data_available'] = self.check_training_data()
            
            # Check models
            status['models_available'] = self.check_trained_models()
            
            # Get counts
            try:
                if self.minio_client.bucket_exists(self.bucket_name):
                    objects = list(self.minio_client.list_objects(self.bucket_name, recursive=True))
                    status['data_file_count'] = len([obj for obj in objects if obj.object_name.endswith(('.json', '.csv'))])
                
                if self.minio_client.bucket_exists(self.model_bucket):
                    objects = list(self.minio_client.list_objects(self.model_bucket, recursive=True))
                    status['model_count'] = len([obj for obj in objects if obj.object_name.endswith('.tar.gz')])
                    
            except Exception as e:
                logger.warning(f"Error getting file counts: {e}")
        
        return status
    
    def run_continuous_monitoring(self, interval=60):
        """Run continuous monitoring"""
        logger.info("Starting continuous monitoring...")
        
        while True:
            try:
                logger.info("=== Spark Trainer Monitor ===")
                status = self.get_system_status()
                
                logger.info(f"MinIO Connected: {status['minio_connected']}")
                logger.info(f"Training Data Available: {status['training_data_available']}")
                logger.info(f"Models Available: {status['models_available']}")
                logger.info(f"Data Files: {status['data_file_count']}")
                logger.info(f"Trained Models: {status['model_count']}")
                
                if status['training_data_available'] and not status['models_available']:
                    logger.info("Training data is available but no models found. Training may be in progress.")
                elif status['models_available']:
                    logger.info("Models are available and ready for serving.")
                else:
                    logger.warning("No training data available yet.")
                
                time.sleep(interval)
                
            except KeyboardInterrupt:
                logger.info("Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(interval)

if __name__ == "__main__":
    monitor = SparkTrainerMonitor()
    
    # Run one-time status check
    if len(os.sys.argv) > 1 and os.sys.argv[1] == '--once':
        status = monitor.get_system_status()
        print(json.dumps(status, indent=2))
    else:
        # Run continuous monitoring
        monitor.run_continuous_monitoring()
