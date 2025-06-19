import subprocess
import time
import signal
import sys
import os

def signal_handler(sig, frame):
    print('Shutting down services...')
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("🦆 Starting DuckDB Query Services...")
    
    # Start FastAPI service
    print("🚀 Starting FastAPI service on port 8002...")
    fastapi_process = subprocess.Popen([
        'python', 'query_service.py'
    ])
    
    # Wait a bit for FastAPI to start
    time.sleep(10)
    
    # Start Streamlit service
    print("🚀 Starting Streamlit interface on port 8502...")
    streamlit_process = subprocess.Popen([
        'streamlit', 'run', 'streamlit_app.py',
        '--server.address', '0.0.0.0',
        '--server.port', '8502',
        '--server.headless', 'true',
        '--browser.gatherUsageStats', 'false'
    ])
    
    print("✅ Services started successfully!")
    print("📊 Services are ready:")
    print("   - FastAPI: http://localhost:8002")
    print("   - Streamlit: http://localhost:8502")
    
    try:
        # Keep both processes running
        fastapi_process.wait()
        streamlit_process.wait()
    except KeyboardInterrupt:
        print("Shutting down...")
        fastapi_process.terminate()
        streamlit_process.terminate()

if __name__ == "__main__":
    main()