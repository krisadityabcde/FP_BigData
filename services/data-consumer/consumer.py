import os
import json
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
import numpy as np

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from minio import Minio
from minio.error import S3Error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HospitalDataConsumer:
    def __init__(self):
        # Kafka configuration
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'hospital-data')
        self.group_id = os.getenv('KAFKA_GROUP_ID', 'hospital-data-consumer')
        
        # MinIO configuration
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.bucket_name = os.getenv('MINIO_BUCKET', 'hospital-data')
        
        # Processing configuration
        self.batch_size = 100  # Number of records to batch before saving
        self.batch_timeout = 30  # Seconds to wait before saving incomplete batch
        
        self.consumer = None
        self.minio_client = None
        self.message_batch = []
        self.last_save_time = datetime.now()
        
    def initialize_kafka_consumer(self):
        """Initialize Kafka consumer with retry logic"""
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=[self.kafka_servers],
                    group_id=self.group_id,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    key_deserializer=lambda k: k.decode('utf-8') if k else None,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,
                    consumer_timeout_ms=10000
                )
                logger.info(f"Successfully connected to Kafka at {self.kafka_servers}")
                return True
            except Exception as e:
                retry_count += 1
                logger.warning(f"Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {e}")
                time.sleep(5)
        
        logger.error("Failed to connect to Kafka after maximum retries")
        return False
    
    def initialize_minio_client(self):
        """Initialize MinIO client and create bucket if not exists"""
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.minio_client = Minio(
                    self.minio_endpoint,
                    access_key=self.minio_access_key,
                    secret_key=self.minio_secret_key,
                    secure=False  # Use HTTP instead of HTTPS for local development
                )
                
                # Test connection
                self.minio_client.list_buckets()
                logger.info(f"Successfully connected to MinIO at {self.minio_endpoint}")
                
                # Create bucket if it doesn't exist
                self.create_bucket()
                return True
                
            except Exception as e:
                retry_count += 1
                logger.warning(f"Failed to connect to MinIO (attempt {retry_count}/{max_retries}): {e}")
                time.sleep(5)
        
        logger.error("Failed to connect to MinIO after maximum retries")
        return False
    
    def create_bucket(self):
        """Create MinIO bucket and folder structure"""
        try:
            # Check if bucket exists
            if not self.minio_client.bucket_exists(self.bucket_name):
                self.minio_client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
            else:
                logger.info(f"Bucket {self.bucket_name} already exists")
            
            # Create initial folder structure
            folders = [
                "raw/",
                "processed/",
                "archived/",
                "year=2015/",
                "year=2015/month=01/",
                "year=2015/month=02/",
                "year=2015/month=03/",
                "year=2015/month=04/",
                "year=2015/month=05/",
                "year=2015/month=06/",
                "year=2015/month=07/",
                "year=2015/month=08/",
                "year=2015/month=09/",
                "year=2015/month=10/",
                "year=2015/month=11/",
                "year=2015/month=12/"
            ]
            
            for folder in folders:
                try:
                    # Create empty object to represent folder
                    self.minio_client.put_object(
                        self.bucket_name,
                        f"{folder}.placeholder",
                        BytesIO(b""),
                        0
                    )
                except S3Error as e:
                    if e.code != 'NoSuchKey':
                        logger.warning(f"Could not create folder {folder}: {e}")
            
            logger.info("Bucket structure created successfully")
            
        except Exception as e:
            logger.error(f"Error creating bucket structure: {e}")
    
    def save_batch_to_minio(self, batch_data, batch_id):
        """Save batch of data to MinIO in multiple formats"""
        try:
            if not batch_data:
                return
            
            # Convert to DataFrame
            df = pd.DataFrame(batch_data)
            timestamp = datetime.now()
            
            # Generate file paths
            base_path = f"raw/year={timestamp.year}/month={timestamp.month:02d}"
            json_path = f"{base_path}/batch_{batch_id}_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
            parquet_path = f"{base_path}/batch_{batch_id}_{timestamp.strftime('%Y%m%d_%H%M%S')}.parquet"
            
            # Save as JSON (original format)
            json_data = json.dumps(batch_data, indent=2, default=str)
            json_bytes = BytesIO(json_data.encode('utf-8'))
            
            self.minio_client.put_object(
                self.bucket_name,
                json_path,
                json_bytes,
                len(json_data.encode('utf-8')),
                content_type='application/json'
            )
            
            # Save as Parquet (optimized format)
            try:
                parquet_buffer = BytesIO()
                df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                parquet_buffer.seek(0)
                
                self.minio_client.put_object(
                    self.bucket_name,
                    parquet_path,
                    parquet_buffer,
                    len(parquet_buffer.getvalue()),
                    content_type='application/octet-stream'
                )
                
                logger.info(f"Saved batch {batch_id} with {len(batch_data)} records to MinIO (JSON & Parquet)")
                
            except Exception as e:
                logger.warning(f"Could not save Parquet format: {e}")
                logger.info(f"Saved batch {batch_id} with {len(batch_data)} records to MinIO (JSON only)")
            
            # Save metadata
            metadata = {
                "batch_id": batch_id,
                "record_count": len(batch_data),
                "timestamp": timestamp.isoformat(),
                "json_path": json_path,
                "parquet_path": parquet_path,
                "data_source": "kafka-stream",
                "schema_version": "1.0"
            }
            
            metadata_path = f"{base_path}/metadata/batch_{batch_id}_metadata.json"
            metadata_json = json.dumps(metadata, indent=2)
            metadata_bytes = BytesIO(metadata_json.encode('utf-8'))
            
            self.minio_client.put_object(
                self.bucket_name,
                metadata_path,
                metadata_bytes,
                len(metadata_json.encode('utf-8')),
                content_type='application/json'
            )
            
        except Exception as e:
            logger.error(f"Error saving batch to MinIO: {e}")
    
    def should_save_batch(self):
        """Determine if current batch should be saved"""
        if len(self.message_batch) >= self.batch_size:
            return True
        
        time_since_last_save = datetime.now() - self.last_save_time
        if time_since_last_save.total_seconds() >= self.batch_timeout and len(self.message_batch) > 0:
            return True
        
        return False
    
    def process_message(self, message):
        """Process individual Kafka message"""
        try:
            # Add processing timestamp
            message_data = message.value
            message_data['kafka_timestamp'] = message.timestamp
            message_data['kafka_offset'] = message.offset
            message_data['kafka_partition'] = message.partition
            message_data['consumer_timestamp'] = datetime.now().isoformat()
            
            # Add to batch
            self.message_batch.append(message_data)
            
            # Check if we should save the batch
            if self.should_save_batch():
                batch_id = f"{int(time.time())}_{len(self.message_batch)}"
                self.save_batch_to_minio(self.message_batch, batch_id)
                self.message_batch = []
                self.last_save_time = datetime.now()
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def consume_messages(self):
        """Main message consumption loop"""
        logger.info("Starting message consumption...")
        
        try:
            message_count = 0
            
            for message in self.consumer:
                self.process_message(message)
                message_count += 1
                
                if message_count % 10 == 0:
                    logger.info(f"Processed {message_count} messages, batch size: {len(self.message_batch)}")
                
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Error during message consumption: {e}")
        finally:
            # Save any remaining messages in batch
            if self.message_batch:
                batch_id = f"final_{int(time.time())}_{len(self.message_batch)}"
                self.save_batch_to_minio(self.message_batch, batch_id)
                logger.info(f"Saved final batch with {len(self.message_batch)} messages")
    
    def list_stored_data(self):
        """List data stored in MinIO for monitoring"""
        try:
            objects = self.minio_client.list_objects(self.bucket_name, recursive=True)
            file_count = 0
            total_size = 0
            
            for obj in objects:
                if not obj.object_name.endswith('.placeholder'):
                    file_count += 1
                    total_size += obj.size
                    logger.info(f"Stored: {obj.object_name} ({obj.size} bytes)")
            
            logger.info(f"Total files in MinIO: {file_count}, Total size: {total_size} bytes")
            
        except Exception as e:
            logger.warning(f"Could not list MinIO objects: {e}")
    
    def run(self):
        """Main execution method"""
        logger.info("Starting Hospital Data Consumer...")
        
        # Wait for services to be ready
        time.sleep(60)  # Give Kafka and MinIO time to start
        
        # Initialize connections
        if not self.initialize_kafka_consumer():
            return False
        
        if not self.initialize_minio_client():
            return False
        
        # List initial state
        self.list_stored_data()
        
        # Start consuming messages
        self.consume_messages()
        
        # Close consumer
        if self.consumer:
            self.consumer.close()
        
        logger.info("Hospital Data Consumer finished")
        return True

if __name__ == "__main__":
    consumer = HospitalDataConsumer()
    consumer.run()
