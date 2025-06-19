#!/usr/bin/env python3
"""
Test script for Hospital Prediction API
Tests all available endpoints and models
"""

import requests
import json
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class APITester:
    def __init__(self, base_url='http://localhost:5001'):
        self.base_url = base_url
        
    def test_health_endpoint(self):
        """Test the health endpoint"""
        try:
            logger.info("Testing health endpoint...")
            response = requests.get(f"{self.base_url}/health")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Health check passed: {data}")
                return True
            else:
                logger.error(f"Health check failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error testing health endpoint: {e}")
            return False
    
    def test_models_endpoint(self):
        """Test the models listing endpoint"""
        try:
            logger.info("Testing models endpoint...")
            response = requests.get(f"{self.base_url}/models")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Models available: {data.get('total_models', 0)}")
                
                for model_name, info in data.get('models', {}).items():
                    logger.info(f"  - {model_name}: {info.get('target_column')} ({info.get('model_type')})")
                
                return data.get('models', {})
            else:
                logger.error(f"Models endpoint failed: {response.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"Error testing models endpoint: {e}")
            return {}
    
    def test_prediction(self, model_name):
        """Test prediction for a specific model"""
        try:
            logger.info(f"Testing prediction for model: {model_name}")
            
            # Sample input data
            test_data = {
                "age_group": "30 to 49",
                "gender": "M",
                "race": "White",
                "ethnicity": "Not Span/Hispanic",
                "admission_type": "Emergency",
                "disposition": "Home or Self Care",
                "diagnosis": "Mood disorders",
                "procedure": "No procedure",
                "severity": "Minor",
                "mortality_risk": "Minor",
                "county": "New York",
                "birth_weight": 0,
                "drg_code": 444,
                "severity_code": 1
            }
            
            response = requests.post(
                f"{self.base_url}/predict/{model_name}",
                json=test_data,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    result = data.get('result', {})
                    prediction = result.get('prediction')
                    logger.info(f"Prediction successful: {prediction}")
                    logger.info(f"Model type: {result.get('model_type')}")
                    logger.info(f"RMSE: {result.get('rmse')}")
                    return True
                else:
                    logger.error(f"Prediction failed: {data}")
                    return False
            else:
                logger.error(f"Prediction request failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error testing prediction for {model_name}: {e}")
            return False
    
    def test_all_predictions(self):
        """Test prediction for all available models"""
        try:
            logger.info("Testing prediction for all models...")
            
            test_data = {
                "age_group": "50 to 69",
                "gender": "F",
                "race": "Black/African American",
                "ethnicity": "Not Span/Hispanic",
                "admission_type": "Elective",
                "disposition": "Home or Self Care",
                "diagnosis": "Diabetes mellitus with complications",
                "procedure": "Diagnostic cardiac catheterization",
                "severity": "Moderate",
                "mortality_risk": "Moderate",
                "county": "Kings",
                "birth_weight": 0,
                "drg_code": 250,
                "severity_code": 2
            }
            
            response = requests.post(
                f"{self.base_url}/predict/all",
                json=test_data,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    results = data.get('results', {})
                    logger.info(f"All predictions completed for {len(results)} models:")
                    
                    for model_name, result in results.items():
                        if 'error' in result:
                            logger.warning(f"  {model_name}: Error - {result['error']}")
                        else:
                            prediction = result.get('prediction')
                            logger.info(f"  {model_name}: {prediction}")
                    
                    return True
                else:
                    logger.error(f"All predictions failed: {data}")
                    return False
            else:
                logger.error(f"All predictions request failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error testing all predictions: {e}")
            return False
    
    def run_comprehensive_test(self):
        """Run comprehensive API test"""
        logger.info("=== Starting Comprehensive API Test ===")
        
        # Test health endpoint
        if not self.test_health_endpoint():
            logger.error("Health check failed, aborting tests")
            return False
        
        # Wait for API to be fully ready
        time.sleep(2)
        
        # Test models endpoint
        models = self.test_models_endpoint()
        if not models:
            logger.warning("No models available for testing")
            return True
        
        # Test individual model predictions
        success_count = 0
        for model_name in models.keys():
            if self.test_prediction(model_name):
                success_count += 1
            time.sleep(1)  # Small delay between tests
        
        logger.info(f"Individual predictions: {success_count}/{len(models)} successful")
        
        # Test all predictions endpoint
        if self.test_all_predictions():
            logger.info("All predictions test passed")
        else:
            logger.warning("All predictions test failed")
        
        logger.info("=== API Test Completed ===")
        return True

def wait_for_api(base_url='http://localhost:5001', timeout=300):
    """Wait for API to become available"""
    logger.info(f"Waiting for API at {base_url} to become available...")
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{base_url}/health", timeout=5)
            if response.status_code == 200:
                logger.info("API is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        
        time.sleep(5)
    
    logger.error(f"API did not become available within {timeout} seconds")
    return False

if __name__ == "__main__":
    import sys
    
    base_url = sys.argv[1] if len(sys.argv) > 1 else 'http://localhost:5001'
    
    # Wait for API to be ready
    if not wait_for_api(base_url):
        sys.exit(1)
    
    # Run tests
    tester = APITester(base_url)
    success = tester.run_comprehensive_test()
    
    sys.exit(0 if success else 1)
