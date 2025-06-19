from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
from duckdb_engine import DuckDBQueryEngine
import logging
from typing import Optional, Dict, Any
import uvicorn

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="DuckDB Query Service",
    description="REST API for querying Parquet files in MinIO using DuckDB",
    version="1.0.0"
)

# Global query engine instance
query_engine = None

@app.on_event("startup")
async def startup_event():
    """Initialize DuckDB connection on startup"""
    global query_engine
    try:
        query_engine = DuckDBQueryEngine()
        logger.info("DuckDB Query Service started successfully")
    except Exception as e:
        logger.error(f"Failed to start query service: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Close DuckDB connection on shutdown"""
    global query_engine
    if query_engine:
        query_engine.close_connection()
        logger.info("DuckDB Query Service shut down")

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"status": "DuckDB Query Service is running", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Detailed health check"""
    try:
        # Test connection with a simple query
        test_result = query_engine.execute_query("SELECT 1 as test")
        return {
            "status": "healthy",
            "duckdb_connection": "active",
            "test_query": "passed"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }

@app.post("/query/csv")
async def query_csv(
    bucket_name: str,
    file_path: str,
    query: Optional[str] = None,
    limit: Optional[int] = Query(default=1000, le=10000)
):
    """
    Query CSV file from MinIO
    
    - **bucket_name**: MinIO bucket name
    - **file_path**: Path to CSV file in bucket (URL encoded paths will be decoded)
    - **query**: SQL query (use {table} as placeholder for the CSV file)
    - **limit**: Maximum number of rows to return
    """
    try:
        if query is None:
            query = f"SELECT * FROM {{table}} LIMIT {limit}"
        elif "LIMIT" not in query.upper():
            query += f" LIMIT {limit}"
            
        result_df = query_engine.query_csv_from_minio(
            bucket_name=bucket_name,
            file_path=file_path,
            query=query
        )
        
        # Convert DataFrame to JSON with proper handling of NaN/infinity values
        import pandas as pd
        import numpy as np
        
        # First replace infinity values with None
        result_df_clean = result_df.replace([np.inf, -np.inf], None)
        
        # Then replace NaN values with None using where method
        result_df_clean = result_df_clean.where(pd.notnull(result_df_clean), None)
        
        # Convert to dict with additional safety check
        result_json = []
        for _, row in result_df_clean.iterrows():
            row_dict = {}
            for col, val in row.items():
                if pd.isna(val) or np.isinf(val) if isinstance(val, (int, float)) else False:
                    row_dict[col] = None
                else:
                    row_dict[col] = val
            result_json.append(row_dict)
        
        return {
            "status": "success",
            "rows_returned": len(result_json),
            "query": query.replace("{table}", f"s3://{bucket_name}/{file_path}"),
            "data": result_json
        }
        
    except Exception as e:
        logger.error(f"CSV Query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query/file")
async def query_file(
    bucket_name: str,
    file_path: str,
    file_type: str = Query(default="csv", regex="^(csv|parquet|auto)$"),
    query: Optional[str] = None,
    limit: Optional[int] = Query(default=1000, le=10000)
):
    """
    Query file from MinIO with auto file type detection
    
    - **bucket_name**: MinIO bucket name
    - **file_path**: Path to file in bucket (URL encoded paths will be decoded)
    - **file_type**: File type (csv, parquet, or auto for auto-detection)
    - **query**: SQL query (use {table} as placeholder)
    - **limit**: Maximum number of rows to return
    """
    try:
        if query is None:
            query = f"SELECT * FROM {{table}} LIMIT {limit}"
        elif "LIMIT" not in query.upper():
            query += f" LIMIT {limit}"
            
        result_df = query_engine.query_file_from_minio(
            bucket_name=bucket_name,
            file_path=file_path,
            query=query,
            file_type=file_type
        )
        
        # Convert DataFrame to JSON with proper handling of NaN/infinity values
        import pandas as pd
        import numpy as np
        
        result_df_clean = result_df.replace([np.inf, -np.inf], None)
        result_df_clean = result_df_clean.where(pd.notnull(result_df_clean), None)
        
        result_json = []
        for _, row in result_df_clean.iterrows():
            row_dict = {}
            for col, val in row.items():
                if pd.isna(val) or np.isinf(val) if isinstance(val, (int, float)) else False:
                    row_dict[col] = None
                else:
                    row_dict[col] = val
            result_json.append(row_dict)
        
        return {
            "status": "success",
            "rows_returned": len(result_json),
            "query": query.replace("{table}", f"s3://{bucket_name}/{file_path}"),
            "file_type": file_type,
            "data": result_json
        }
        
    except Exception as e:
        logger.error(f"File Query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/schema/{bucket_name}")
async def get_schema(bucket_name: str, file_path: str, file_type: str = Query(default="csv")):
    """
    Get schema information of file
    
    - **bucket_name**: MinIO bucket name
    - **file_path**: Path to file in bucket
    - **file_type**: File type (csv or parquet)
    """
    try:
        schema_df = query_engine.get_table_info(
            bucket_name=bucket_name,
            file_path=file_path,
            file_type=file_type
        )
        
        schema_json = schema_df.to_dict(orient='records')
        
        return {
            "status": "success",
            "bucket": bucket_name,
            "file_path": file_path,
            "file_type": file_type,
            "schema": schema_json
        }
        
    except Exception as e:
        logger.error(f"Schema retrieval failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Update existing parquet endpoint to handle URL decoding
@app.post("/query/parquet")
async def query_parquet(
    bucket_name: str,
    file_path: str,
    query: Optional[str] = None,
    limit: Optional[int] = Query(default=1000, le=10000)
):
    """
    Query Parquet file from MinIO
    
    - **bucket_name**: MinIO bucket name
    - **file_path**: Path to Parquet file in bucket (URL encoded paths will be decoded)
    - **query**: SQL query (use {table} as placeholder for the parquet file)
    - **limit**: Maximum number of rows to return
    """
    try:
        if query is None:
            query = f"SELECT * FROM {{table}} LIMIT {limit}"
        elif "LIMIT" not in query.upper():
            query += f" LIMIT {limit}"
            
        result_df = query_engine.query_parquet_from_minio(
            bucket_name=bucket_name,
            file_path=file_path,
            query=query
        )
        
        # Convert DataFrame to JSON with proper handling of NaN/infinity values
        import pandas as pd
        import numpy as np
        
        result_df_clean = result_df.replace([np.inf, -np.inf], None)
        result_df_clean = result_df_clean.where(pd.notnull(result_df_clean), None)
        
        result_json = []
        for _, row in result_df_clean.iterrows():
            row_dict = {}
            for col, val in row.items():
                if pd.isna(val) or np.isinf(val) if isinstance(val, (int, float)) else False:
                    row_dict[col] = None
                else:
                    row_dict[col] = val
            result_json.append(row_dict)
        
        return {
            "status": "success",
            "rows_returned": len(result_json),
            "query": query.replace("{table}", f"s3://{bucket_name}/{file_path}"),
            "data": result_json
        }
        
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/view/create")
async def create_view(
    view_name: str,
    bucket_name: str,
    file_path: str
):
    """
    Create a view from Parquet file for easier querying
    
    - **view_name**: Name for the view
    - **bucket_name**: MinIO bucket name
    - **file_path**: Path to Parquet file in bucket
    """
    try:
        query_engine.create_view_from_parquet(
            view_name=view_name,
            bucket_name=bucket_name,
            file_path=file_path
        )
        
        return {
            "status": "success",
            "message": f"View '{view_name}' created successfully",
            "view_name": view_name,
            "source": f"s3://{bucket_name}/{file_path}"
        }
        
    except Exception as e:
        logger.error(f"View creation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query/view")
async def query_view(
    view_name: str,
    query: Optional[str] = None,
    limit: Optional[int] = Query(default=1000, le=10000)
):
    """
    Query an existing view
    
    - **view_name**: Name of the view to query
    - **query**: SQL query (if not provided, will select all from view)
    - **limit**: Maximum number of rows to return
    """
    try:
        if query is None:
            query = f"SELECT * FROM {view_name} LIMIT {limit}"
        elif "LIMIT" not in query.upper():
            query += f" LIMIT {limit}"
            
        result_df = query_engine.execute_query(query)
        result_json = result_df.to_dict(orient='records')
        
        return {
            "status": "success",
            "rows_returned": len(result_json),
            "query": query,
            "data": result_json
        }
        
    except Exception as e:
        logger.error(f"View query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query/sql")
async def execute_sql(
    sql_query: str,
    limit: Optional[int] = Query(default=1000, le=10000)
):
    """
    Execute arbitrary SQL query
    
    - **sql_query**: SQL query to execute
    - **limit**: Maximum number of rows to return
    """
    try:
        if "LIMIT" not in sql_query.upper():
            sql_query += f" LIMIT {limit}"
            
        result_df = query_engine.execute_query(sql_query)
        result_json = result_df.to_dict(orient='records')
        
        return {
            "status": "success",
            "rows_returned": len(result_json),
            "query": sql_query,
            "data": result_json
        }
        
    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(
        "query_service:app",
        host="0.0.0.0",
        port=8002,
        reload=True,
        log_level="info"
    )
