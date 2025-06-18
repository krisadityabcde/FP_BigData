@echo off
REM Utility script for managing the Big Data system (Windows)

if "%1"=="" goto help
if "%1"=="help" goto help
if "%1"=="start" goto start
if "%1"=="stop" goto stop
if "%1"=="restart" goto restart
if "%1"=="logs" goto logs
if "%1"=="status" goto status
if "%1"=="monitor" goto monitor
if "%1"=="kafka-ui" goto kafka_ui
if "%1"=="minio-ui" goto minio_ui
if "%1"=="streamlit" goto streamlit
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
echo   stop          Stop all services
echo   restart       Restart all services
echo   logs          Show logs for all services
echo   status        Show status of all services
echo   monitor       Monitor MinIO storage
echo   kafka-ui      Open Kafka UI in browser
echo   minio-ui      Open MinIO Console in browser
echo   streamlit     Open Streamlit Monitor in browser
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
echo   - Streamlit Monitor: http://localhost:8501
echo.
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

:monitor
echo 📊 Monitoring MinIO storage...
docker exec -it data-consumer python monitor.py
goto end

:kafka_ui
echo 🌐 Opening Kafka UI...
start http://localhost:8080
goto end

:minio_ui
echo 🌐 Opening MinIO Console...
start http://localhost:9090
goto end

:streamlit
echo 🌐 Opening Streamlit Monitor...
start http://localhost:8501
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
