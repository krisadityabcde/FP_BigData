import duckdb
import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import logging
import urllib.parse

# Load environment variables
load_dotenv()

class DuckDBQueryEngine:
    def __init__(self):
        self.conn = None
        self.setup_connection()
        
    def setup_connection(self):
        """Setup DuckDB connection with S3/MinIO configuration"""
        try:
            # Create DuckDB connection
            self.conn = duckdb.connect(':memory:')
            
            # Install and load required extensions
            self.conn.execute("INSTALL httpfs;")
            self.conn.execute("LOAD httpfs;")
            
            # Configure S3/MinIO settings - use localhost for direct access
            minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9090')
            minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
            minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
            
            # Set S3 configuration for MinIO
            logging.info(f"Configuring DuckDB with MinIO endpoint: {minio_endpoint}")
            
            self.conn.execute(f"""
                SET s3_endpoint = '{minio_endpoint}';
                SET s3_access_key_id = '{minio_access_key}';
                SET s3_secret_access_key = '{minio_secret_key}';
                SET s3_use_ssl = false;
                SET s3_url_style = 'path';
                SET s3_region = 'us-east-1';
            """)
            
            logging.info("DuckDB connection established with MinIO configuration")
            
        except Exception as e:
            logging.error(f"Failed to setup DuckDB connection: {e}")
            raise
    
    def execute_query(self, query):
        """Execute SQL query and return results"""
        try:
            result = self.conn.execute(query).fetchdf()
            return result
        except Exception as e:
            logging.error(f"Query execution failed: {e}")
            raise
    
    def query_csv_from_minio(self, bucket_name, file_path, query=None):
        """Query CSV file directly from MinIO"""
        try:
            # Properly decode URL-encoded path
            decoded_path = urllib.parse.unquote(file_path)
            
            # Construct S3 path for MinIO
            s3_path = f"s3://{bucket_name}/{decoded_path}"
            
            if query is None:
                # Default query to select all data from CSV
                query = f"SELECT * FROM read_csv_auto('{s3_path}')"
            else:
                # Replace table placeholder with CSV read function
                query = query.replace("{table}", f"read_csv_auto('{s3_path}')")
            
            logging.info(f"Executing query on {s3_path}")
            result = self.execute_query(query)
            return result
            
        except Exception as e:
            logging.error(f"Failed to query CSV from MinIO: {e}")
            raise
    
    def query_parquet_from_minio(self, bucket_name, file_path, query=None):
        """Query Parquet file directly from MinIO (kept for backward compatibility)"""
        try:
            # Properly decode URL-encoded path
            decoded_path = urllib.parse.unquote(file_path)
            
            # Construct S3 path for MinIO
            s3_path = f"s3://{bucket_name}/{decoded_path}"
            
            if query is None:
                # Default query to select all data
                query = f"SELECT * FROM read_parquet('{s3_path}')"
            else:
                # Replace table placeholder with parquet read function
                query = query.replace("{table}", f"read_parquet('{s3_path}')")
            
            logging.info(f"Executing query on {s3_path}")
            result = self.execute_query(query)
            return result
            
        except Exception as e:
            logging.error(f"Failed to query parquet from MinIO: {e}")
            raise
    
    def query_file_from_minio(self, bucket_name, file_path, query=None, file_type="csv"):
        """Query file from MinIO with auto-detection of file type"""
        try:
            # Properly decode URL-encoded path
            decoded_path = urllib.parse.unquote(file_path)
            
            # Auto-detect file type if not specified
            if file_type == "auto":
                if decoded_path.lower().endswith('.parquet'):
                    file_type = "parquet"
                elif decoded_path.lower().endswith('.csv'):
                    file_type = "csv"
                else:
                    file_type = "csv"  # Default to CSV
            
            # Construct S3 path for MinIO
            s3_path = f"s3://{bucket_name}/{decoded_path}"
            
            if query is None:
                # Default query based on file type
                if file_type == "parquet":
                    query = f"SELECT * FROM read_parquet('{s3_path}')"
                else:
                    query = f"SELECT * FROM read_csv_auto('{s3_path}')"
            else:
                # Replace table placeholder with appropriate read function
                if file_type == "parquet":
                    query = query.replace("{table}", f"read_parquet('{s3_path}')")
                else:
                    query = query.replace("{table}", f"read_csv_auto('{s3_path}')")
            
            logging.info(f"Executing {file_type.upper()} query on {s3_path}")
            result = self.execute_query(query)
            return result
            
        except Exception as e:
            logging.error(f"Failed to query {file_type} from MinIO: {e}")
            raise
    
    def create_view_from_file(self, view_name, bucket_name, file_path, file_type="csv"):
        """Create a view from file for easier querying"""
        try:
            decoded_path = urllib.parse.unquote(file_path)
            s3_path = f"s3://{bucket_name}/{decoded_path}"
            
            if file_type == "parquet":
                create_view_query = f"""
                    CREATE OR REPLACE VIEW {view_name} AS 
                    SELECT * FROM read_parquet('{s3_path}')
                """
            else:
                create_view_query = f"""
                    CREATE OR REPLACE VIEW {view_name} AS 
                    SELECT * FROM read_csv_auto('{s3_path}')
                """
                
            self.conn.execute(create_view_query)
            logging.info(f"View '{view_name}' created successfully")
            
        except Exception as e:
            logging.error(f"Failed to create view: {e}")
            raise
    
    def get_table_info(self, bucket_name, file_path, file_type="csv"):
        """Get schema information of file"""
        try:
            decoded_path = urllib.parse.unquote(file_path)
            s3_path = f"s3://{bucket_name}/{decoded_path}"
            
            if file_type == "parquet":
                info_query = f"DESCRIBE SELECT * FROM read_parquet('{s3_path}')"
            else:
                info_query = f"DESCRIBE SELECT * FROM read_csv_auto('{s3_path}')"
                
            result = self.execute_query(info_query)
            return result
            
        except Exception as e:
            logging.error(f"Failed to get table info: {e}")
            raise
    
    def close_connection(self):
        """Close DuckDB connection"""
        if self.conn:
            self.conn.close()
            logging.info("DuckDB connection closed")

# Example usage functions
def run_sample_queries():
    """Run sample queries to demonstrate functionality"""
    query_engine = DuckDBQueryEngine()
    
    try:
        # Example 1: Query all data from a parquet file
        print("=== Sample Query 1: Select All Data ===")
        result1 = query_engine.query_parquet_from_minio(
            bucket_name="hospital-data",
            file_path="processed/sample_data.parquet"
        )
        print(result1.head())
        
        # Example 2: Query with conditions
        print("\n=== Sample Query 2: Query with Conditions ===")
        custom_query = """
        SELECT column1, column2, COUNT(*) as count
        FROM {table}
        WHERE column1 > 100
        GROUP BY column1, column2
        ORDER BY count DESC
        LIMIT 10
        """
        result2 = query_engine.query_parquet_from_minio(
            bucket_name="data-lake",
            file_path="processed/sample_data.parquet",
            query=custom_query
        )
        print(result2)
        
        # Example 3: Create view and query it
        print("\n=== Sample Query 3: Using Views ===")
        query_engine.create_view_from_parquet(
            view_name="sample_view",
            bucket_name="data-lake",
            file_path="processed/sample_data.parquet"
        )
        
        view_query = "SELECT * FROM sample_view LIMIT 5"
        result3 = query_engine.execute_query(view_query)
        print(result3)
        
        # Example 4: Get table schema
        print("\n=== Sample Query 4: Table Schema ===")
        schema_info = query_engine.get_table_info(
            bucket_name="data-lake",
            file_path="processed/sample_data.parquet"
        )
        print(schema_info)
        
    except Exception as e:
        print(f"Error running sample queries: {e}")
    
    finally:
        query_engine.close_connection()

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Run sample queries
    run_sample_queries()
