import os
import time
import json
import logging
import pandas as pd
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
        
        # Multiple possible paths for dataset
        possible_paths = [
            '/app/data',  # Docker container path
            os.path.join(os.path.dirname(__file__), 'data'),  # Relative to script
            './data',  # Current directory
            'data'  # Simple relative path
        ]
        
        self.dataset_path = None
        for path in possible_paths:
            if os.path.exists(path):
                self.dataset_path = path
                break
        
        # If no existing path found, use the first one (Docker path)
        if self.dataset_path is None:
            self.dataset_path = possible_paths[0]
        
        logger.info(f"Using dataset path: {self.dataset_path}")
        self.producer = None

    def initialize_kafka_producer(self):
        """Initialize Kafka producer with retry logic"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432
            )
            logger.info("Kafka producer initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Error initializing Kafka producer: {e}")
            return False

    def check_dataset_exists(self):
        """Check if dataset exists in local data folder"""
        try:
            logger.info(f"Checking dataset path: {self.dataset_path}")
            logger.info(f"Current working directory: {os.getcwd()}")
            logger.info(f"Script directory: {os.path.dirname(__file__)}")
            
            # List all directories in current path for debugging
            try:
                current_dir_contents = os.listdir('.')
                logger.info(f"Current directory contents: {current_dir_contents}")
            except Exception as e:
                logger.warning(f"Could not list current directory: {e}")
            
            if not os.path.exists(self.dataset_path):
                logger.error(f"Data folder does not exist: {self.dataset_path}")
                
                # Try to create the directory
                try:
                    os.makedirs(self.dataset_path, exist_ok=True)
                    logger.info(f"Created data directory: {self.dataset_path}")
                except Exception as e:
                    logger.error(f"Could not create data directory: {e}")
                    return False
            
            # Check if directory is readable
            if not os.access(self.dataset_path, os.R_OK):
                logger.error(f"Data folder is not readable: {self.dataset_path}")
                return False
            
            # List all files in the data directory for debugging
            try:
                all_files = os.listdir(self.dataset_path)
                logger.info(f"All files in data directory: {all_files}")
            except Exception as e:
                logger.error(f"Could not list files in data directory: {e}")
                return False
            
            # Find CSV files in the data directory
            csv_files = []
            for file in all_files:
                if file.lower().endswith('.csv'):
                    full_path = os.path.join(self.dataset_path, file)
                    if os.path.isfile(full_path):
                        csv_files.append(full_path)
                        logger.info(f"Found CSV file: {file} (size: {os.path.getsize(full_path)} bytes)")
            
            if not csv_files:
                logger.error(f"No CSV files found in {self.dataset_path}")
                logger.error("Please ensure your CSV files are placed in the data folder")
                logger.error("Make sure the files have .csv extension (case-insensitive)")
                return False
            
            logger.info(f"Found {len(csv_files)} CSV file(s) in {self.dataset_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error checking dataset: {e}")
            return False

    def load_and_process_data(self):
        """Load and process the hospital data"""
        try:
            # Find CSV files in the dataset directory
            csv_files = []
            for file in os.listdir(self.dataset_path):
                if file.lower().endswith('.csv'):
                    full_path = os.path.join(self.dataset_path, file)
                    if os.path.isfile(full_path):
                        csv_files.append(full_path)
            
            if not csv_files:
                logger.error("No CSV files found in dataset")
                return None
            
            logger.info(f"Found CSV files: {[os.path.basename(f) for f in csv_files]}")
            
            # Load the main dataset (usually the largest CSV file)
            main_file = max(csv_files, key=os.path.getsize)
            logger.info(f"Loading main dataset from: {os.path.basename(main_file)}")
            logger.info(f"File size: {os.path.getsize(main_file)} bytes")
            
            # Test if file can be read
            try:
                test_df = pd.read_csv(main_file, nrows=5)
                logger.info(f"Dataset preview - Shape: {test_df.shape}, Columns: {list(test_df.columns)}")
            except Exception as e:
                logger.error(f"Error reading CSV file: {e}")
                return None
            
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
            data['source'] = 'local_hospital_dataset'
            
            return data
            
        except Exception as e:
            logger.warning(f"Error cleaning row: {e}")
            return None

    def create_kafka_topics(self):
        """Create necessary Kafka topics"""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_servers,
                client_id='hospital_data_admin'
            )
            
            topic_list = [NewTopic(name=self.topic, num_partitions=3, replication_factor=1)]
            
            try:
                admin_client.create_topics(new_topics=topic_list, validate_only=False)
                logger.info(f"Topic '{self.topic}' created successfully")
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.info(f"Topic '{self.topic}' already exists")
                else:
                    logger.warning(f"Error creating topic: {e}")
            
        except Exception as e:
            logger.warning(f"Could not create Kafka topics: {e}")

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
        logger.debug(f"Message delivered to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

    def on_send_error(self, excp):
        """Callback for failed message delivery"""
        logger.error(f"Failed to deliver message: {excp}")

    def run(self):
        """Main execution method"""
        try:
            logger.info("Starting Hospital Data Producer...")
            
            # Check if dataset exists locally
            if not self.check_dataset_exists():
                logger.error("Dataset not found. Please place your CSV files in the 'data' folder")
                return
            
            # Initialize Kafka producer
            if not self.initialize_kafka_producer():
                return
            
            # Create Kafka topics
            self.create_kafka_topics()
            
            # Stream data
            success = self.stream_data()
            
            if success:
                logger.info("Data streaming completed successfully")
            else:
                logger.error("Data streaming failed")
                
        except KeyboardInterrupt:
            logger.info("Stopping producer due to keyboard interrupt...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            if self.producer:
                self.producer.close()
                logger.info("Producer closed")

if __name__ == "__main__":
    producer = HospitalDataProducer()
    producer.run()
