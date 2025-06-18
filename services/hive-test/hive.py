from pyhive import hive
import pandas as pd

def query_hospital_data():
    try:
        conn = hive.Connection(host='localhost', port=9083)
        
        # Refresh metadata (tell Hive about new files)
        cursor = conn.cursor()
        cursor.execute("MSCK REPAIR TABLE hospital_data")
        
        print("🔍 Querying your streaming hospital data...")
        
        # Query 1: Count total records
        df_count = pd.read_sql("SELECT COUNT(*) as total_records FROM hospital_data", conn)
        print(f"📊 Total records streamed: {df_count.iloc[0]['total_records']}")
        
        # Query 2: Recent data
        df_recent = pd.read_sql("""
            SELECT facility_name, age_group, total_charges, processed_timestamp
            FROM hospital_data 
            ORDER BY processed_timestamp DESC 
            LIMIT 10
        """, conn)
        print(f"\n📋 Most recent 10 records:")
        print(df_recent)
        
        # Query 3: Summary statistics
        df_stats = pd.read_sql("""
            SELECT 
                facility_name,
                COUNT(*) as patient_count,
                AVG(total_charges) as avg_charges,
                MAX(processed_timestamp) as latest_timestamp
            FROM hospital_data 
            GROUP BY facility_name
            ORDER BY patient_count DESC
            LIMIT 5
        """, conn)
        print(f"\n📈 Top facilities by patient count:")
        print(df_stats)
        
        conn.close()
        return df_recent, df_stats
        
    except Exception as e:
        print(f"❌ Error querying data: {e}")
        return None, None

if __name__ == "__main__":
    query_hospital_data()