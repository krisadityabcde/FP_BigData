import streamlit as st
import requests
import pandas as pd
import json
from typing import Optional, Dict, Any
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configuration
QUERY_SERVICE_URL = os.getenv('QUERY_SERVICE_URL', 'http://localhost:8002')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DuckDBStreamlitInterface:
    def __init__(self):
        self.query_service_url = QUERY_SERVICE_URL
    
    def check_service_health(self) -> bool:
        """Check if the query service is available"""
        try:
            response = requests.get(f"{self.query_service_url}/health", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def query_file(self, bucket_name: str, file_path: str, 
                   file_type: str = "csv", query: Optional[str] = None, 
                   limit: int = 1000) -> Dict[str, Any]:
        """Query file through the API"""
        try:
            payload = {
                "bucket_name": bucket_name,
                "file_path": file_path,
                "file_type": file_type,
                "limit": limit
            }
            if query:
                payload["query"] = query
                
            response = requests.post(
                f"{self.query_service_url}/query/file",
                params=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {"status": "error", "message": response.text}
                
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def query_parquet(self, bucket_name: str, file_path: str, 
                      query: Optional[str] = None, limit: int = 1000) -> Dict[str, Any]:
        """Query Parquet file through the API"""
        try:
            payload = {
                "bucket_name": bucket_name,
                "file_path": file_path,
                "limit": limit
            }
            if query:
                payload["query"] = query
                
            response = requests.post(
                f"{self.query_service_url}/query/parquet",
                params=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {"status": "error", "message": response.text}
                
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def query_csv(self, bucket_name: str, file_path: str, 
                  query: Optional[str] = None, limit: int = 1000) -> Dict[str, Any]:
        """Query CSV file through the API"""
        try:
            payload = {
                "bucket_name": bucket_name,
                "file_path": file_path,
                "limit": limit
            }
            if query:
                payload["query"] = query
                
            response = requests.post(
                f"{self.query_service_url}/query/csv",
                params=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {"status": "error", "message": response.text}
                
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def get_schema(self, bucket_name: str, file_path: str, file_type: str = "csv") -> Dict[str, Any]:
        """Get schema information"""
        try:
            response = requests.get(
                f"{self.query_service_url}/schema/{bucket_name}",
                params={"file_path": file_path, "file_type": file_type},
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {"status": "error", "message": response.text}
                
        except Exception as e:
            return {"status": "error", "message": str(e)}

def main():
    st.set_page_config(
        page_title="DuckDB Query Interface",
        page_icon="🦆",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🦆 DuckDB Query Interface")
    st.markdown("Query your data files stored in MinIO using DuckDB")
    
    # Initialize interface
    interface = DuckDBStreamlitInterface()
    
    # Check service health
    if not interface.check_service_health():
        st.error("❌ Query service is not available. Please ensure the DuckDB query service is running.")
        st.info(f"Expected service URL: {QUERY_SERVICE_URL}")
        return
    
    st.success("✅ Query service is running")
    
    # Sidebar for configuration
    st.sidebar.header("Query Configuration")
    
    bucket_name = st.sidebar.text_input(
        "MinIO Bucket Name",
        value="hospital-data",
        help="Name of the MinIO bucket containing your files"
    )
    
    file_path = st.sidebar.text_input(
        "File Path",
        value="raw/year=2025/month=06",
        help="Path to the file within the bucket (supports URL encoded paths)"
    )
    
    file_type = st.sidebar.selectbox(
        "File Type",
        ["csv", "parquet", "auto"],
        index=0,
        help="Type of file to query"
    )
    
    query_limit = st.sidebar.slider(
        "Query Limit",
        min_value=10,
        max_value=10000,
        value=1000,
        help="Maximum number of rows to return"
    )
    
    # Main content area
    tab1, tab2, tab3, tab4 = st.tabs(["🔍 Query Data", "📋 Schema Info", "📊 Quick Analysis", "💡 Sample Queries"])
    
    with tab1:
        st.header(f"Query {file_type.upper()} Data")
        
        # Schema info button
        if st.button("📋 Show Schema", key="schema_btn"):
            with st.spinner("Fetching schema information..."):
                schema_result = interface.get_schema(bucket_name, file_path, file_type)
                
                if schema_result.get("status") == "success":
                    schema_df = pd.DataFrame(schema_result["schema"])
                    st.subheader("Schema Information")
                    st.dataframe(schema_df, use_container_width=True)
                else:
                    st.error(f"Error fetching schema: {schema_result.get('message')}")
        
        # Query input
        query_mode = st.radio(
            "Query Mode",
            ["Simple Query (SELECT ALL)", "Custom SQL Query"],
            horizontal=True
        )
        
        if query_mode == "Custom SQL Query":
            st.info(f"💡 Use `{{table}}` as placeholder for the {file_type.upper()} file in your query")
            custom_query = st.text_area(
                "SQL Query",
                value="SELECT * FROM {table} WHERE column_name > 100 ORDER BY column_name LIMIT 10",
                height=100,
                help=f"Write your SQL query here. Use {{table}} to reference the {file_type.upper()} file."
            )
        else:
            custom_query = None
        
        # Execute query
        if st.button("🚀 Execute Query", type="primary"):
            with st.spinner("Executing query..."):
                # Use the appropriate method based on file type
                if file_type == "parquet":
                    result = interface.query_parquet(
                        bucket_name=bucket_name,
                        file_path=file_path,
                        query=custom_query,
                        limit=query_limit
                    )
                elif file_type == "csv":
                    result = interface.query_csv(
                        bucket_name=bucket_name,
                        file_path=file_path,
                        query=custom_query,
                        limit=query_limit
                    )
                else:  # auto or unified
                    result = interface.query_file(
                        bucket_name=bucket_name,
                        file_path=file_path,
                        file_type=file_type,
                        query=custom_query,
                        limit=query_limit
                    )
                
                if result.get("status") == "success":
                    data = result["data"]
                    df = pd.DataFrame(data)
                    
                    st.success(f"✅ Query executed successfully! Returned {result['rows_returned']} rows")
                    
                    # Display executed query
                    with st.expander("📝 Executed Query"):
                        st.code(result["query"], language="sql")
                    
                    # Display results
                    if not df.empty:
                        st.subheader("Query Results")
                        st.dataframe(df, use_container_width=True)
                        
                        # Download button
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Results as CSV",
                            data=csv,
                            file_name="query_results.csv",
                            mime="text/csv"
                        )
                    else:
                        st.info("No data returned from query")
                        
                else:
                    st.error(f"❌ Query failed: {result.get('message')}")

    with tab2:
        st.header("📋 Schema Information")
        
        if st.button("🔍 Get Schema", key="get_schema_tab2"):
            with st.spinner("Fetching schema information..."):
                schema_result = interface.get_schema(bucket_name, file_path, file_type)
                
                if schema_result.get("status") == "success":
                    st.success("✅ Schema retrieved successfully")
                    
                    # Display file info
                    st.subheader("File Information")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Bucket", schema_result["bucket"])
                    with col2:
                        st.metric("File Type", schema_result["file_type"].upper())
                    with col3:
                        st.metric("Columns", len(schema_result["schema"]))
                    
                    # Display schema
                    st.subheader("Column Schema")
                    schema_df = pd.DataFrame(schema_result["schema"])
                    st.dataframe(schema_df, use_container_width=True)
                    
                    # Download schema
                    schema_csv = schema_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Schema as CSV",
                        data=schema_csv,
                        file_name="schema_info.csv",
                        mime="text/csv"
                    )
                else:
                    st.error(f"❌ Error fetching schema: {schema_result.get('message')}")

    with tab3:
        st.header("📊 Quick Analysis")
        
        analysis_type = st.selectbox(
            "Select Analysis Type",
            ["Row Count", "Column Statistics", "Data Preview", "Null Value Analysis"]
        )
        
        if st.button("🔍 Run Analysis", key="run_analysis"):
            with st.spinner("Running analysis..."):
                if analysis_type == "Row Count":
                    query = "SELECT COUNT(*) as total_rows FROM {table}"
                elif analysis_type == "Column Statistics":
                    query = "SELECT * FROM {table} LIMIT 0"  # Just to get schema, then we'll build stats query
                elif analysis_type == "Data Preview":
                    query = "SELECT * FROM {table} LIMIT 10"
                elif analysis_type == "Null Value Analysis":
                    # This will be built dynamically after getting schema
                    query = "SELECT * FROM {table} LIMIT 0"
                
                result = interface.query_file(
                    bucket_name=bucket_name,
                    file_path=file_path,
                    file_type=file_type,
                    query=query,
                    limit=query_limit
                )
                
                if result.get("status") == "success":
                    data = result["data"]
                    df = pd.DataFrame(data)
                    
                    st.success(f"✅ Analysis completed!")
                    
                    if analysis_type == "Row Count":
                        if not df.empty:
                            st.metric("Total Rows", df.iloc[0]['total_rows'])
                    elif analysis_type == "Data Preview":
                        st.subheader("Data Preview (First 10 rows)")
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.dataframe(df, use_container_width=True)
                        
                else:
                    st.error(f"❌ Analysis failed: {result.get('message')}")

    with tab4:
        st.header("💡 Sample Queries")
        
        st.markdown("""
        Here are some sample queries you can use. Remember to use `{table}` as placeholder for your file:
        """)
        
        sample_queries = {
            "Basic Selection": "SELECT * FROM {table} LIMIT 100",
            "Count Records": "SELECT COUNT(*) as total_records FROM {table}",
            "Filter Data": "SELECT * FROM {table} WHERE column_name > 100",
            "Group By": "SELECT column_name, COUNT(*) as count FROM {table} GROUP BY column_name",
            "Order By": "SELECT * FROM {table} ORDER BY column_name DESC LIMIT 50",
            "Aggregate Functions": "SELECT AVG(numeric_column), MAX(numeric_column), MIN(numeric_column) FROM {table}",
            "Date Filtering": "SELECT * FROM {table} WHERE date_column >= '2025-01-01'",
            "String Operations": "SELECT * FROM {table} WHERE text_column LIKE '%pattern%'"
        }
        
        for query_name, query_sql in sample_queries.items():
            with st.expander(f"📝 {query_name}"):
                st.code(query_sql, language="sql")

if __name__ == "__main__":
    main()
