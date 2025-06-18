import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import json
from typing import Dict, Any, Optional
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
QUERY_SERVICE_URL = "http://duckdb-query:8002"

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
    
    def get_schema(self, bucket_name: str, file_path: str) -> Dict[str, Any]:
        """Get schema information"""
        try:
            response = requests.get(
                f"{self.query_service_url}/schema/{bucket_name}",
                params={"file_path": file_path},
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
        layout="wide"
    )
    
    st.title("🦆 DuckDB Query Interface for MinIO Parquet Files")
    st.markdown("Interactive SQL querying of Parquet files stored in MinIO using DuckDB")
    
    # Initialize interface
    interface = DuckDBStreamlitInterface()
    
    # Check service health
    with st.spinner("Checking DuckDB Query Service..."):
        service_healthy = interface.check_service_health()
    
    if not service_healthy:
        st.error("❌ DuckDB Query Service is not available. Please ensure the service is running.")
        return
    
    st.success("✅ DuckDB Query Service is running")
    
    # Sidebar for configuration
    st.sidebar.header("Query Configuration")
    
    bucket_name = st.sidebar.text_input(
        "MinIO Bucket Name",
        value="data-lake",
        help="Name of the MinIO bucket containing your Parquet files"
    )
    
    file_path = st.sidebar.text_input(
        "Parquet File Path",
        value="processed/sample_data.parquet",
        help="Path to the Parquet file within the bucket"
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
        st.header("Query Parquet Data")
        
        # Schema info button
        if st.button("📋 Show Schema", key="schema_btn"):
            with st.spinner("Fetching schema information..."):
                schema_result = interface.get_schema(bucket_name, file_path)
                
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
            st.info("💡 Use `{table}` as placeholder for the Parquet file in your query")
            custom_query = st.text_area(
                "SQL Query",
                value="SELECT * FROM {table} WHERE column_name > 100 ORDER BY column_name LIMIT 10",
                height=100,
                help="Write your SQL query here. Use {table} to reference the Parquet file."
            )
        else:
            custom_query = None
        
        # Execute query
        if st.button("🚀 Execute Query", type="primary"):
            with st.spinner("Executing query..."):
                result = interface.query_parquet(
                    bucket_name=bucket_name,
                    file_path=file_path,
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
        st.header("Schema Information")
        st.markdown("Get detailed schema information about your Parquet file")
        
        if st.button("🔍 Fetch Schema", key="fetch_schema_tab2"):
            with st.spinner("Fetching schema..."):
                schema_result = interface.get_schema(bucket_name, file_path)
                
                if schema_result.get("status") == "success":
                    schema_df = pd.DataFrame(schema_result["schema"])
                    
                    st.subheader(f"Schema for: `s3://{bucket_name}/{file_path}`")
                    st.dataframe(schema_df, use_container_width=True)
                    
                    # Schema summary
                    st.subheader("Schema Summary")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Total Columns", len(schema_df))
                    
                    with col2:
                        data_types = schema_df['column_type'].value_counts() if 'column_type' in schema_df.columns else {}
                        st.metric("Unique Data Types", len(data_types))
                    
                    with col3:
                        nullable_count = schema_df['null'].sum() if 'null' in schema_df.columns else 0
                        st.metric("Nullable Columns", nullable_count)
                    
                else:
                    st.error(f"Error fetching schema: {schema_result.get('message')}")
    
    with tab3:
        st.header("Quick Data Analysis")
        st.markdown("Perform quick statistical analysis on your data")
        
        analysis_type = st.selectbox(
            "Select Analysis Type",
            ["Basic Statistics", "Column Distribution", "Data Sample", "Row Count"]
        )
        
        if st.button("📊 Run Analysis", key="run_analysis"):
            with st.spinner("Running analysis..."):
                if analysis_type == "Basic Statistics":
                    query = "SELECT COUNT(*) as total_rows FROM {table}"
                elif analysis_type == "Column Distribution":
                    query = "SELECT * FROM {table} LIMIT 100"
                elif analysis_type == "Data Sample":
                    query = "SELECT * FROM {table} LIMIT 10"
                elif analysis_type == "Row Count":
                    query = "SELECT COUNT(*) as row_count FROM {table}"
                
                result = interface.query_parquet(
                    bucket_name=bucket_name,
                    file_path=file_path,
                    query=query,
                    limit=10000 if analysis_type == "Column Distribution" else 1000
                )
                
                if result.get("status") == "success":
                    df = pd.DataFrame(result["data"])
                    
                    if analysis_type == "Data Sample":
                        st.subheader("Data Sample (First 10 rows)")
                        st.dataframe(df, use_container_width=True)
                        
                    elif analysis_type == "Row Count":
                        st.subheader("Row Count")
                        st.metric("Total Rows", df.iloc[0]['row_count'] if not df.empty else 0)
                        
                    elif analysis_type == "Column Distribution":
                        st.subheader("Column Distribution Analysis")
                        
                        # Select numeric columns for visualization
                        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                        
                        if numeric_cols:
                            selected_col = st.selectbox("Select column to visualize", numeric_cols)
                            
                            if selected_col:
                                # Create histogram
                                fig = px.histogram(
                                    df, 
                                    x=selected_col, 
                                    title=f"Distribution of {selected_col}",
                                    nbins=30
                                )
                                st.plotly_chart(fig, use_container_width=True)
                                
                                # Basic statistics
                                col1, col2, col3, col4 = st.columns(4)
                                with col1:
                                    st.metric("Mean", f"{df[selected_col].mean():.2f}")
                                with col2:
                                    st.metric("Median", f"{df[selected_col].median():.2f}")
                                with col3:
                                    st.metric("Std Dev", f"{df[selected_col].std():.2f}")
                                with col4:
                                    st.metric("Count", len(df[selected_col].dropna()))
                        else:
                            st.info("No numeric columns found for visualization")
                            st.dataframe(df.describe(), use_container_width=True)
                            
                else:
                    st.error(f"Analysis failed: {result.get('message')}")
    
    with tab4:
        st.header("Sample Queries")
        st.markdown("Here are some example queries you can try:")
        
        sample_queries = [
            {
                "title": "Select All Data",
                "description": "Get all data from the Parquet file",
                "query": "SELECT * FROM {table} LIMIT 100"
            },
            {
                "title": "Count Rows",
                "description": "Count total number of rows",
                "query": "SELECT COUNT(*) as total_rows FROM {table}"
            },
            {
                "title": "Group By Analysis",
                "description": "Group data by a column and count",
                "query": "SELECT column_name, COUNT(*) as count FROM {table} GROUP BY column_name ORDER BY count DESC LIMIT 10"
            },
            {
                "title": "Filtered Data",
                "description": "Filter data based on conditions",
                "query": "SELECT * FROM {table} WHERE column_name > 100 AND another_column IS NOT NULL LIMIT 50"
            },
            {
                "title": "Statistical Summary",
                "description": "Get basic statistics for numeric columns",
                "query": "SELECT AVG(numeric_column) as avg_value, MIN(numeric_column) as min_value, MAX(numeric_column) as max_value FROM {table}"
            },
            {
                "title": "Date Range Query",
                "description": "Query data within a date range",
                "query": "SELECT * FROM {table} WHERE date_column >= '2023-01-01' AND date_column <= '2023-12-31' LIMIT 100"
            }
        ]
        
        for i, query_info in enumerate(sample_queries):
            with st.expander(f"📝 {query_info['title']}"):
                st.markdown(f"**Description:** {query_info['description']}")
                st.code(query_info['query'], language="sql")
                
                if st.button(f"▶️ Run Query", key=f"sample_query_{i}"):
                    with st.spinner("Executing sample query..."):
                        result = interface.query_parquet(
                            bucket_name=bucket_name,
                            file_path=file_path,
                            query=query_info['query'],
                            limit=query_limit
                        )
                        
                        if result.get("status") == "success":
                            df = pd.DataFrame(result["data"])
                            st.success(f"✅ Returned {len(df)} rows")
                            st.dataframe(df, use_container_width=True)
                        else:
                            st.error(f"❌ Query failed: {result.get('message')}")

if __name__ == "__main__":
    main()
