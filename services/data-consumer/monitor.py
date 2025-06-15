import os
import time
from minio import Minio
from minio.error import S3Error

def monitor_minio():
    """Monitor MinIO storage and display statistics"""
    
    # MinIO configuration
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    bucket_name = os.getenv('MINIO_BUCKET', 'hospital-data')
    
    try:
        # Initialize MinIO client
        client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )
        
        print(f"📊 MinIO Storage Monitor - Bucket: {bucket_name}")
        print("=" * 60)
        
        # List objects and calculate statistics
        objects = client.list_objects(bucket_name, recursive=True)
        
        stats = {
            'json_files': 0,
            'parquet_files': 0,
            'metadata_files': 0,
            'other_files': 0,
            'total_size': 0,
            'total_files': 0
        }
        
        print("\n📁 Files in storage:")
        print("-" * 60)
        
        for obj in objects:
            if obj.object_name.endswith('.placeholder'):
                continue
                
            stats['total_files'] += 1
            stats['total_size'] += obj.size
            
            # Categorize files
            if obj.object_name.endswith('.json'):
                if 'metadata' in obj.object_name:
                    stats['metadata_files'] += 1
                    file_type = "METADATA"
                else:
                    stats['json_files'] += 1
                    file_type = "JSON"
            elif obj.object_name.endswith('.parquet'):
                stats['parquet_files'] += 1
                file_type = "PARQUET"
            else:
                stats['other_files'] += 1
                file_type = "OTHER"
            
            # Format size
            size_mb = obj.size / (1024 * 1024)
            size_str = f"{size_mb:.2f} MB" if size_mb > 1 else f"{obj.size} bytes"
            
            print(f"{file_type:8} | {obj.object_name:40} | {size_str:>10}")
        
        print("\n📈 Statistics:")
        print("-" * 60)
        print(f"Total Files:      {stats['total_files']}")
        print(f"JSON Files:       {stats['json_files']}")
        print(f"Parquet Files:    {stats['parquet_files']}")
        print(f"Metadata Files:   {stats['metadata_files']}")
        print(f"Other Files:      {stats['other_files']}")
        print(f"Total Size:       {stats['total_size'] / (1024 * 1024):.2f} MB")
        
        return stats
        
    except Exception as e:
        print(f"❌ Error monitoring MinIO: {e}")
        return None

if __name__ == "__main__":
    monitor_minio()
