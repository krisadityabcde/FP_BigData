@echo off
REM DuckDB Query Service Management Script for Windows

echo DuckDB Query Service Management
echo ================================

if "%1"=="api" (
    echo Starting DuckDB Query API...
    python query_service.py
) else if "%1"=="streamlit" (
    echo Starting Streamlit Interface...
    streamlit run streamlit_app.py --server.host 0.0.0.0 --server.port 8501
) else if "%1"=="samples" (
    echo Running Sample Queries...
    python duckdb_engine.py
) else if "%1"=="test" (
    echo Testing DuckDB Connection...
    python -c "from duckdb_engine import DuckDBQueryEngine; engine = DuckDBQueryEngine(); result = engine.execute_query('SELECT 1 as test'); print('Connection successful!'); print(result); engine.close_connection()"
) else (
    echo Usage: %0 {api^|streamlit^|samples^|test}
    echo.
    echo Commands:
    echo   api       - Start the REST API service
    echo   streamlit - Start the Streamlit web interface
    echo   samples   - Run sample queries
    echo   test      - Test DuckDB connection
    echo.
)
