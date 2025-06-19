@echo off
REM Setup script for Big Data Final Project (Windows)

echo === Big Data ^& Data Lakehouse Final Project Setup ===
echo.

REM Check if Docker is installed
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker is not installed. Please install Docker first.
    exit /b 1
)

REM Check if Docker Compose is installed
docker-compose --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker Compose is not installed. Please install Docker Compose first.
    exit /b 1
)

echo ✅ Docker and Docker Compose are installed

REM Check if .env file exists
if not exist .env (
    echo ⚠️  .env file not found. Creating from template...
    copy .env.example .env
    echo 📝 Please edit .env file with your Kaggle credentials before running the system
    echo    Get your credentials from: https://www.kaggle.com/settings/account
    echo.
)

REM Create data directory
if not exist data mkdir data
echo ✅ Data directory created

REM Pull Docker images
echo 📥 Pulling Docker images...
docker-compose pull

REM Setup Streamlit Monitor service
cd services\streamlit-monitor
start /b cmd /c "uv sync"
echo 📦 Streamlit Monitor service setup complete

cd ..\..
echo 📦 All services setup complete
