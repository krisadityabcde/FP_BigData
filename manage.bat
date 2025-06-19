@echo off
REM Utility script for managing the Big Data system (Windows)

if "%1"=="" goto help
if "%1"=="help" goto help
if "%1"=="start" goto start
if "%1"=="start-pipeline" goto start_pipeline
if "%1"=="stop" goto stop
if "%1"=="restart" goto restart
if "%1"=="logs" goto logs
if "%1"=="status" goto status
if "%1"=="monitor" goto monitor
if "%1"=="debug" goto debug
if "%1"=="check-data" goto check_data
if "%1"=="train" goto train
if "%1"=="test-data" goto test_data
if "%1"=="api-test" goto api_test
if "%1"=="kafka-ui" goto kafka_ui
if "%1"=="minio-ui" goto minio_ui
if "%1"=="clean" goto clean

echo ❌ Unknown command: %1
echo.
goto help

:help
echo Big Data System Management Script
echo.
echo Usage: manage.bat [COMMAND]
echo.
echo Commands:
echo   start         Start all services
echo   start-pipeline Start services in proper order for data flow
echo   stop          Stop all services
echo   restart       Restart all services
echo   logs          Show logs for all services
echo   status        Show status of all services
echo   monitor       Monitor MinIO storage
echo   debug         Debug specific service issues
echo   check-data    Check data availability in MinIO
echo   train         Manually trigger ML training
echo   test-data     Test data loading functionality
echo   api-test      Test the prediction API
echo   kafka-ui      Open Kafka UI in browser
echo   minio-ui      Open MinIO Console in browser
echo   clean         Clean up all data and volumes
echo   help          Show this help message
echo.
goto end

:start
echo 🚀 Starting Big Data services...
docker-compose up -d --build
echo ✅ Services started successfully!
echo.
echo 🌐 Available services:
echo   - Kafka UI: http://localhost:8080
echo   - MinIO Console: http://localhost:9090
echo   - Prediction API: http://localhost:5001
echo.
echo 💡 Use 'manage.bat check-data' to verify data availability
goto end

:stop
echo 🛑 Stopping Big Data services...
docker-compose down
echo ✅ Services stopped successfully!
goto end

:restart
echo 🔄 Restarting Big Data services...
docker-compose down
docker-compose up -d --build
echo ✅ Services restarted successfully!
goto end

:logs
echo 📋 Showing logs for all services...
docker-compose logs -f --tail=100
goto end

:status
echo 📊 Service Status:
echo.
docker-compose ps
echo.
echo 📈 Resource Usage:
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"
goto end

:start_pipeline
echo 🚀 Starting complete data pipeline...
echo 1. Starting infrastructure services...
docker-compose up -d zookeeper kafka minio kafka-ui
echo    Waiting for services to be ready...
timeout /t 15 /nobreak >nul
echo 2. Starting data producer...
docker-compose up -d data-producer
echo    Waiting for data production to start...
timeout /t 10 /nobreak >nul
echo 3. Starting data consumer...
docker-compose up -d data-consumer
echo    Waiting for data consumption to start...
timeout /t 15 /nobreak >nul
echo 4. Starting Streamlit Monitor...
cd services\streamlit-monitor
start /b cmd /c "uv run streamlit run app.py --server.port 8501"
echo    Waiting for Streamlit Monitor to be ready...
cd ..\..
echo 5. Starting ML trainer...
docker-compose up -d spark-trainer
timeout /t 240 /nobreak >nul
echo 6. Starting API service...
docker-compose up -d api
echo ✅ Complete pipeline started!
echo 💡 Monitor progress with: manage.bat debug
goto end

:debug
echo 🔍 Debugging system issues...
echo.
echo === Service Status ===
docker-compose ps
echo.
echo === Recent Logs ===
echo Data Producer logs:
docker-compose logs --tail=10 data-producer
echo.
echo Data Consumer logs:
docker-compose logs --tail=10 data-consumer
echo.
echo Spark Trainer logs:
docker-compose logs --tail=10 spark-trainer
echo.
echo API logs:
docker-compose logs --tail=10 api
goto end

:check_data
echo 📊 Checking data availability...
echo.
echo === MinIO Buckets ===
docker exec -it minio mc ls minio/ 2>nul || echo MinIO not accessible
echo.
echo === Data Files ===
docker-compose run --rm data-consumer python -c "import os; from minio import Minio; client = Minio('minio:9000', access_key='minioadmin', secret_key='minioadmin', secure=False); objects = list(client.list_objects('hospital-data', recursive=True)); print(f'Found {len(objects)} files in hospital-data bucket:'); [print(f'  - {obj.object_name} ({obj.size} bytes)') for obj in objects[:10]]; print(f'  ... and {len(objects)-10} more files') if len(objects) > 10 else None"
goto end

:train
echo 🚀 Manually triggering ML training...
docker-compose restart spark-trainer
echo Training started. Monitor with: docker-compose logs -f spark-trainer
goto end

:test_data
echo 🧪 Testing data loading functionality...
docker-compose run --rm spark-trainer python test_data_loading.py
goto end

:api_test
echo 🧪 Testing Prediction API...
echo Waiting for API to be ready...
timeout /t 5 /nobreak >nul
docker-compose run --rm api python test_api.py http://api:5001
goto end

:monitor
echo 📊 Monitoring MinIO storage...
docker ps --format "{{.Names}}" | findstr "spark-monitor" >nul
if %errorlevel% equ 0 (
    docker exec -it spark-monitor python monitor.py --once
) else (
    echo Starting monitoring container...
    docker-compose run --rm spark-trainer python monitor.py --once
)
goto end

:kafka_ui
echo 🌐 Opening Kafka UI...
start http://localhost:8080
goto end

:minio_ui
echo 🌐 Opening MinIO Console...
start http://localhost:9090
goto end

:clean
echo 🧹 Cleaning up system...
set /p confirm="This will remove all containers, volumes, and data. Are you sure? (y/N): "
if /i "%confirm%"=="y" (
    docker-compose down -v --remove-orphans
    docker system prune -f
    echo ✅ System cleaned successfully!
) else (
    echo ❌ Cleanup cancelled
)
goto end

:end
