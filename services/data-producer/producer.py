import os
import time
import json
import logging
import pandas as pd
import kagglehub
from kafka import KafkaProducer
from kafka.errors import KafkaError
import numpy as np
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HospitalDataProducer:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'hospital-data')
        self.dataset_path = None
        self.producer = None
        
    def initialize_kafka_producer(self):
        """Initialize Kafka producer with retry logic"""
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[self.kafka_servers],
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                    key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    batch_size=16384,
                    linger_ms=10,
                    buffer_memory=33554432
                )
                logger.info(f"Successfully connected to Kafka at {self.kafka_servers}")
                return True
            except Exception as e:
                retry_count += 1
                logger.warning(f"Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {e}")
                time.sleep(5)
        
        logger.error("Failed to connect to Kafka after maximum retries")
        return False
    
    def download_dataset(self):
        """Download dataset using kagglehub"""
        try:
            logger.info("Starting dataset download from Kaggle...")
            
            # Download the dataset
            path = kagglehub.dataset_download("jonasalmeida/2015-deidentified-ny-inpatient-discharge-sparcs")
            
            logger.info(f"Dataset downloaded to: {path}")
            self.dataset_path = path
            
            # List files in the downloaded directory
            import os
            files = os.listdir(path)
            logger.info(f"Available files: {files}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error downloading dataset: {e}")
            return False
    
    def load_and_process_data(self):
        """Load and process the hospital data"""
        try:
            # Find CSV files in the dataset directory
            csv_files = []
            for root, dirs, files in os.walk(self.dataset_path):
                for file in files:
                    if file.endswith('.csv'):
                        csv_files.append(os.path.join(root, file))
            
            if not csv_files:
                logger.error("No CSV files found in dataset")
                return None
            
            logger.info(f"Found CSV files: {csv_files}")
            
            # Load the main dataset (usually the largest CSV file)
            main_file = max(csv_files, key=os.path.getsize)
            logger.info(f"Loading main dataset from: {main_file}")
            
            # Load data in chunks to handle large files
            chunk_size = 1000
            df_chunks = pd.read_csv(main_file, chunksize=chunk_size, low_memory=False)
            
            return df_chunks
            
        except Exception as e:
            logger.error(f"Error loading dataset: {e}")
            return None
    
    def clean_data_row(self, row):
        """Clean and prepare a single row of data"""
        try:
            # Convert pandas Series to dict
            data = row.to_dict()
            
            # Handle NaN values
            for key, value in data.items():
                if pd.isna(value):
                    data[key] = None
                elif isinstance(value, (np.integer, np.floating)):
                    data[key] = float(value) if not np.isnan(value) else None
            
            # Add metadata
            data['processed_timestamp'] = datetime.now().isoformat()
            data['source'] = 'ny_inpatient_discharge_2015'
            
            return data
            
        except Exception as e:
            logger.warning(f"Error cleaning row: {e}")
            return None
    
    def create_kafka_topics(self):
        """Create necessary Kafka topics"""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=[self.kafka_servers]
            )
            
            topics = [
                NewTopic(name=self.topic, num_partitions=3, replication_factor=1),
                NewTopic(name=f"{self.topic}-errors", num_partitions=1, replication_factor=1)
            ]
            
            # Create topics
            try:
                admin_client.create_topics(topics)
                logger.info("Kafka topics created successfully")
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.info("Kafka topics already exist")
                else:
                    logger.warning(f"Error creating topics: {e}")
            
        except Exception as e:
            logger.warning(f"Could not create topics: {e}")
    
    def stream_data(self):
        """Stream data to Kafka"""
        try:
            logger.info("Starting data streaming...")
            
            df_chunks = self.load_and_process_data()
            if df_chunks is None:
                return False
            
            total_records = 0
            error_count = 0
            
            for chunk_idx, chunk in enumerate(df_chunks):
                logger.info(f"Processing chunk {chunk_idx + 1} with {len(chunk)} records")
                
                for idx, row in chunk.iterrows():
                    try:
                        # Clean the data
                        cleaned_data = self.clean_data_row(row)
                        
                        if cleaned_data is None:
                            error_count += 1
                            continue
                        
                        # Create message key (you can customize this)
                        key = f"record_{total_records}"
                        
                        # Send to Kafka
                        future = self.producer.send(
                            self.topic,
                            key=key,
                            value=cleaned_data
                        )
                        
                        # Optional: Add callback for delivery confirmation
                        future.add_callback(self.on_send_success)
                        future.add_errback(self.on_send_error)
                        
                        total_records += 1
                        
                        # Log progress every 100 records
                        if total_records % 100 == 0:
                            logger.info(f"Streamed {total_records} records so far...")
                        
                        # Add small delay to simulate real-time streaming
                        time.sleep(0.01)  # 10ms delay
                        
                    except Exception as e:
                        error_count += 1
                        logger.warning(f"Error processing record {idx}: {e}")
                
                # Flush after each chunk
                self.producer.flush()
                logger.info(f"Completed chunk {chunk_idx + 1}")
            
            logger.info(f"Data streaming completed. Total records: {total_records}, Errors: {error_count}")
            return True
            
        except Exception as e:
            logger.error(f"Error during data streaming: {e}")
            return False
    
    def on_send_success(self, record_metadata):
        """Callback for successful message delivery"""
        pass  # You can add logging here if needed
    
    def on_send_error(self, excp):
        """Callback for failed message delivery"""
        logger.error(f"Failed to deliver message: {excp}")
    
    def run(self):
        """Main execution method"""
        logger.info("Starting Hospital Data Producer...")
        
        # Wait for Kafka to be ready
        time.sleep(30)  # Give Kafka time to start
        
        # Initialize Kafka producer
        if not self.initialize_kafka_producer():
            return False
        
        # Create topics
        self.create_kafka_topics()
        
        # Download dataset
        if not self.download_dataset():
            logger.error("Failed to download dataset")
            return False
        
        # Stream data
        success = self.stream_data()
        
        # Close producer
        if self.producer:
            self.producer.close()
        
        logger.info("Hospital Data Producer finished")
        return success

if __name__ == "__main__":
    producer = HospitalDataProducer()
    producer.run()
