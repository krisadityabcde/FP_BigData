import os
import json
import logging
import tarfile
import tempfile
from datetime import datetime
from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType
from pyspark.sql.functions import col, when
from minio import Minio
from minio.error import S3Error
import numpy as np
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

class HospitalPredictionAPI:
    def __init__(self):
        # Spark configuration
        self.spark = None
        
        # MinIO configuration
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.model_bucket = os.getenv('MINIO_MODEL_BUCKET', 'hospital-models')
        
        self.minio_client = None
        self.models = {}
        self.model_metadata = {}
        
        # Initialize services
        self.initialize_services()
    
    def initialize_services(self):
        """Initialize Spark and MinIO connections"""
        try:
            # Initialize Spark
            self.spark = SparkSession.builder \
                .appName("HospitalPredictionAPI") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            logger.info("Spark session initialized")
            
            # Initialize MinIO
            self.minio_client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=False
            )
            
            logger.info("MinIO client initialized")
            
            # Load models
            self.load_models()
            
        except Exception as e:
            logger.error(f"Error initializing services: {e}")
    
    def load_models(self):
        """Load trained models from MinIO"""
        try:
            logger.info("Loading models from MinIO...")
            
            # List all model files
            objects = self.minio_client.list_objects(self.model_bucket, prefix="models/", recursive=True)
            model_files = [obj.object_name for obj in objects if obj.object_name.endswith('.tar.gz')]
            
            for model_file in model_files:
                try:
                    # Extract model name
                    model_name = os.path.basename(model_file).replace('.tar.gz', '')
                    
                    # Load model metadata
                    metadata_path = f"metadata/{model_name}_metadata.json"
                    try:
                        metadata_response = self.minio_client.get_object(self.model_bucket, metadata_path)
                        metadata = json.loads(metadata_response.read().decode('utf-8'))
                        self.model_metadata[model_name] = metadata
                    except Exception as e:
                        logger.warning(f"Could not load metadata for {model_name}: {e}")
                        continue
                    
                    # Download and extract model
                    with tempfile.TemporaryDirectory() as temp_dir:
                        # Download model file
                        model_tar_path = os.path.join(temp_dir, f"{model_name}.tar.gz")
                        self.minio_client.fget_object(self.model_bucket, model_file, model_tar_path)
                        
                        # Extract model
                        with tarfile.open(model_tar_path, "r:gz") as tar:
                            tar.extractall(temp_dir)
                        
                        # Load Spark ML model
                        model_dir = os.path.join(temp_dir, model_name)
                        if os.path.exists(model_dir):
                            model = PipelineModel.load(model_dir)
                            self.models[model_name] = model
                            logger.info(f"Loaded model: {model_name}")
                        else:
                            logger.warning(f"Model directory not found: {model_dir}")
                
                except Exception as e:
                    logger.error(f"Error loading model {model_file}: {e}")
                    continue
            
            logger.info(f"Loaded {len(self.models)} models successfully")
            
        except Exception as e:
            logger.error(f"Error loading models: {e}")
    
    def preprocess_input(self, input_data, target_model):
        """Preprocess input data for prediction"""
        try:
            # Get feature columns from model metadata
            if target_model not in self.model_metadata:
                raise ValueError(f"Model metadata not found for {target_model}")
            
            feature_columns = self.model_metadata[target_model].get('feature_columns', [])
            
            # Create a DataFrame with the input data
            # Fill missing features with default values
            processed_data = {}
            
            # Default values for common categorical fields based on training data
            defaults = {
                'Age_Group': '30 to 49',  # Common age group
                'Gender': 'M',  # Default gender  
                'Race': 'White',  # Most common race in training data
                'Ethnicity': 'Not Span/Hispanic',  # Most common ethnicity
                'Type_of_Admission': 'Emergency',  # Most common admission type
                'Patient_Disposition': 'Home or Self Care',  # Most common disposition
                'CCS_Diagnosis_Description': 'Other digestive system diagnoses',  # Default diagnosis
                'CCS_Procedure_Description': 'NO PROC',  # Default procedure
                'APR_DRG_Description': 'Other digestive system diagnoses',  # Will be overridden by smart mapping
                'APR_Medical_Surgical_Description': 'Medical',  # Most common
                'APR_Severity_of_Illness_Description': 'Minor',  # Will be overridden by smart mapping
                'APR_Risk_of_Mortality': 'Minor',  # Most common risk level
                'Hospital_County': 'New York',  # Common county
                'Operating_Certificate_Number': '1204001.0',  # Use a certificate number from training data
                'Birth_Weight_numeric': 0.0,
                'APR_DRG_Code_numeric': 444.0,  # Will be overridden by input
                'APR_Severity_of_Illness_Code_numeric': 1.0  # Will be overridden by input
            }
            
            # Map input data to expected format
            field_mapping = {
                'age_group': 'Age_Group',
                'gender': 'Gender',
                'race': 'Race', 
                'ethnicity': 'Ethnicity',
                'admission_type': 'Type_of_Admission',
                'disposition': 'Patient_Disposition',
                'diagnosis': 'CCS_Diagnosis_Description',
                'procedure': 'CCS_Procedure_Description',
                'drg_description': 'APR_DRG_Description',
                'medical_surgical': 'APR_Medical_Surgical_Description',
                'severity': 'APR_Severity_of_Illness_Description',
                'mortality_risk': 'APR_Risk_of_Mortality',
                'county': 'Hospital_County',
                'hospital_id': 'Operating_Certificate_Number',
                'birth_weight': 'Birth_Weight_numeric',
                'drg_code': 'APR_DRG_Code_numeric',
                'severity_code': 'APR_Severity_of_Illness_Code_numeric'
            }
            
            # Apply defaults first
            for col, default_val in defaults.items():
                processed_data[col] = default_val
            
            # Override with provided input data, ensuring proper type conversion
            for input_key, input_value in input_data.items():
                if input_key in field_mapping:
                    target_field = field_mapping[input_key]
                    if target_field.endswith('_numeric'):
                        # Convert to float for numeric fields
                        try:
                            processed_data[target_field] = float(input_value) if input_value is not None else 0.0
                        except (ValueError, TypeError):
                            processed_data[target_field] = 0.0
                    else:
                        # String fields
                        processed_data[target_field] = str(input_value) if input_value is not None else 'Unknown'
            
            # Smart mapping: if DRG code is provided, try to get the description
            if 'drg_code' in input_data and input_data['drg_code']:
                try:
                    drg_code = int(input_data['drg_code'])
                    drg_description = self.get_drg_description(drg_code)
                    processed_data['APR_DRG_Description'] = drg_description
                    logger.info(f"Mapped DRG code {drg_code} to '{drg_description}'")
                except:
                    pass
            
            # Smart mapping: if severity code is provided, try to get the description  
            if 'severity_code' in input_data and input_data['severity_code']:
                try:
                    severity_code = int(input_data['severity_code'])
                    severity_description = self.get_severity_description(severity_code)
                    processed_data['APR_Severity_of_Illness_Description'] = severity_description
                    logger.info(f"Mapped severity code {severity_code} to '{severity_description}'")
                except:
                    pass
            
            # Define the schema explicitly to avoid type issues
            schema = StructType([
                StructField("Age_Group", StringType(), True),
                StructField("Gender", StringType(), True),
                StructField("Race", StringType(), True),
                StructField("Ethnicity", StringType(), True),
                StructField("Type_of_Admission", StringType(), True),
                StructField("Patient_Disposition", StringType(), True),
                StructField("CCS_Diagnosis_Description", StringType(), True),
                StructField("CCS_Procedure_Description", StringType(), True),
                StructField("APR_DRG_Description", StringType(), True),
                StructField("APR_Medical_Surgical_Description", StringType(), True),
                StructField("APR_Severity_of_Illness_Description", StringType(), True),
                StructField("APR_Risk_of_Mortality", StringType(), True),
                StructField("Hospital_County", StringType(), True),
                StructField("Operating_Certificate_Number", StringType(), True),
                StructField("Birth_Weight_numeric", FloatType(), True),
                StructField("APR_DRG_Code_numeric", FloatType(), True),
                StructField("APR_Severity_of_Illness_Code_numeric", FloatType(), True)
            ])
            
            # Create Spark DataFrame with explicit schema
            df = self.spark.createDataFrame([processed_data], schema)
            
            return df
            
        except Exception as e:
            logger.error(f"Error preprocessing input: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            raise
    
    def predict(self, model_name, input_data):
        """Make prediction using specified model"""
        try:
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")
            
            logger.info(f"Making prediction with {model_name} for input: {input_data}")
            
            # Use smart auto-fill for minimal inputs
            smart_input = self.create_smart_prediction_input(input_data)
            
            # Preprocess input
            df = self.preprocess_input(smart_input, model_name)
            
            # Show preprocessed data for debugging
            logger.info("Preprocessed data schema:")
            df.printSchema()
            logger.info("Preprocessed data sample:")
            df.show(1, truncate=False)
            
            # Make prediction
            model = self.models[model_name]
            logger.info(f"Model stages: {[stage.__class__.__name__ for stage in model.stages]}")
            
            # Transform data through the pipeline step by step for debugging
            try:
                # Use the model to transform - this includes StringIndexers that might fail
                predictions = model.transform(df)
                
                # Check if predictions DataFrame is empty
                if predictions.count() == 0:
                    raise ValueError("No predictions generated - input data may contain unseen categorical values")
                
                # Extract prediction - handle potential empty results
                prediction_rows = predictions.select("prediction").collect()
                if not prediction_rows:
                    raise ValueError("No prediction rows returned - model may have filtered out input data")
                
                prediction_value = float(prediction_rows[0].prediction)
                
                return {
                    'model': model_name,
                    'prediction': prediction_value,
                    'model_type': self.model_metadata[model_name].get('model_type', 'Unknown'),
                    'rmse': self.model_metadata[model_name].get('rmse', 'Unknown'),
                    'timestamp': datetime.now().isoformat()
                }
                
            except IndexError as ie:
                logger.error(f"IndexError during prediction (likely unseen categorical values): {ie}")
                raise ValueError(f"Cannot make prediction - input contains categorical values not seen during training: {str(ie)}")
            except Exception as transform_error:
                logger.error(f"Error during model transformation: {transform_error}")
                logger.error(f"Model expects features: {self.model_metadata[model_name].get('feature_columns', [])}")
                
                # Check if it's a StringIndexer issue
                if "StringIndexer" in str(transform_error) or "does not exist" in str(transform_error):
                    raise ValueError(f"Input contains categorical values not seen during training: {str(transform_error)}")
                else:
                    raise ValueError(f"Model transformation failed: {str(transform_error)}")
            
        except Exception as e:
            logger.error(f"Error making prediction: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            raise
    
    def get_drg_description(self, drg_code):
        """Map DRG code to description based on training data"""
        # Common DRG code mappings from the training data
        drg_mapping = {
            753: "Bipolar disorders",
            765: "Cesarean delivery", 
            141: "Asthma",
            53: "Seizure",
            774: "Vaginal delivery",
            885: "Schizophrenia", 
            312: "Syncope & collapse",
            751: "Depression except major depressive disorder",
            175: "Pulmonary embolism",
            313: "Chest pain",
            753: "Bipolar disorders",
            291: "Heart failure",
            391: "Abdominal pain",
            460: "RENAL FAILURE",
            202: "Bronchiolitis & RSV pneumonia",
            637: "Diabetes",
            750: "Major depressive disorders & other/unspecified psychoses",
            540: "Other antepartum diagnoses",
            444: "Other digestive system diagnoses"  # This matches the user's example
        }
        return drg_mapping.get(drg_code, "Other digestive system diagnoses")
    
    def get_severity_description(self, severity_code):
        """Map severity code to description"""
        severity_mapping = {
            1: "Minor",
            2: "Moderate", 
            3: "Major",
            4: "Extreme"
        }
        return severity_mapping.get(severity_code, "Minor")
    
    def get_drg_defaults(self, drg_code, severity_code=1):
        """Get reasonable defaults for categorical fields based on DRG code and severity"""
        
        # Common DRG code mappings with their typical characteristics
        drg_mappings = {
            # Mental Health
            753: {
                'description': 'Bipolar disorders',
                'diagnosis': 'Mood disorders',
                'procedure': 'NO PROC',
                'medical_surgical': 'Medical',
                'typical_age': '30 to 49',
                'typical_admission': 'Emergency',
                'typical_disposition': 'Home or Self Care'
            },
            754: {
                'description': 'Schizophrenia',
                'diagnosis': 'Schizophrenia and other psychotic disorders',
                'procedure': 'NO PROC',
                'medical_surgical': 'Medical',
                'typical_age': '30 to 49',
                'typical_admission': 'Emergency',
                'typical_disposition': 'Home or Self Care'
            },
            
            # Respiratory
            141: {
                'description': 'Asthma',
                'diagnosis': 'Asthma',
                'procedure': 'NO PROC',
                'medical_surgical': 'Medical',
                'typical_age': '30 to 49',
                'typical_admission': 'Emergency',
                'typical_disposition': 'Home or Self Care'
            },
            194: {
                'description': 'Simple pneumonia',
                'diagnosis': 'Pneumonia (except that caused by tuberculosis or sexually transmitted disease)',
                'procedure': 'NO PROC',
                'medical_surgical': 'Medical',
                'typical_age': '50 to 69',
                'typical_admission': 'Emergency',
                'typical_disposition': 'Home or Self Care'
            },
            
            # Digestive
            444: {
                'description': 'Other digestive system diagnoses',
                'diagnosis': 'Other gastrointestinal disorders',
                'procedure': 'NO PROC',
                'medical_surgical': 'Medical',
                'typical_age': '50 to 69',
                'typical_admission': 'Emergency',
                'typical_disposition': 'Home or Self Care'
            },
            
            # Neurological
            53: {
                'description': 'Seizure',
                'diagnosis': 'Epilepsy; convulsions',
                'procedure': 'NO PROC',
                'medical_surgical': 'Medical',
                'typical_age': '30 to 49',
                'typical_admission': 'Emergency',
                'typical_disposition': 'Home or Self Care'
            },
            
            # Maternal/Childbirth
            540: {
                'description': 'Cesarean delivery',
                'diagnosis': 'Liveborn',
                'procedure': 'Cesarean section',
                'medical_surgical': 'Surgical',
                'typical_age': '18 to 29',
                'typical_admission': 'Elective',
                'typical_disposition': 'Home or Self Care'
            },
            560: {
                'description': 'Vaginal delivery',
                'diagnosis': 'Liveborn', 
                'procedure': 'Episiotomy',
                'medical_surgical': 'Medical',
                'typical_age': '18 to 29',
                'typical_admission': 'Emergency',
                'typical_disposition': 'Home or Self Care'
            },
            
            # Cardiovascular
            280: {
                'description': 'Acute myocardial infarction',
                'diagnosis': 'Acute myocardial infarction',
                'procedure': 'Cardiac catheterization; coronary arteriography',
                'medical_surgical': 'Medical',
                'typical_age': '50 to 69',
                'typical_admission': 'Emergency',
                'typical_disposition': 'Home or Self Care'
            },
            
            # Default for unknown DRG codes
            'default': {
                'description': 'Other diagnoses',
                'diagnosis': 'Other aftercare',
                'procedure': 'NO PROC',
                'medical_surgical': 'Medical',
                'typical_age': '30 to 49',
                'typical_admission': 'Emergency',
                'typical_disposition': 'Home or Self Care'
            }
        }
        
        # Get DRG-specific defaults or use general defaults
        drg_info = drg_mappings.get(drg_code, drg_mappings['default'])
        
        # Map severity code to severity description
        severity_mapping = {
            1: 'Minor',
            2: 'Moderate', 
            3: 'Major',
            4: 'Extreme'
        }
        
        # Map severity to mortality risk (generally correlates)
        mortality_mapping = {
            1: 'Minor',
            2: 'Moderate',
            3: 'Major', 
            4: 'Extreme'
        }
        
        severity_desc = severity_mapping.get(severity_code, 'Minor')
        mortality_risk = mortality_mapping.get(severity_code, 'Minor')
        
        return {
            'age_group': drg_info['typical_age'],
            'gender': 'F',  # Default, user can override
            'race': 'White',  # Most common in dataset
            'ethnicity': 'Not Span/Hispanic',  # Most common in dataset
            'admission_type': drg_info['typical_admission'],
            'disposition': drg_info['typical_disposition'],
            'diagnosis': drg_info['diagnosis'],
            'procedure': drg_info['procedure'],
            'drg_description': drg_info['description'],
            'medical_surgical': drg_info['medical_surgical'],
            'severity': severity_desc,
            'mortality_risk': mortality_risk,
            'county': 'New York',  # Default county
            'hospital_id': '1001',  # Default hospital
            'birth_weight': 0,  # Default for non-birth cases
            'drg_code': drg_code,
            'severity_code': severity_code
        }

    def create_smart_prediction_input(self, user_input):
        """Create a complete prediction input with smart defaults based on minimal user input"""
        # Extract DRG code and severity code from user input
        drg_code = user_input.get('drg_code', 444)  # Default DRG code
        severity_code = user_input.get('severity_code', 1)  # Default severity
        
        # Get smart defaults based on DRG code
        smart_defaults = self.get_drg_defaults(drg_code, severity_code)
        
        # Override defaults with any user-provided values
        for key, value in user_input.items():
            if key in smart_defaults:
                smart_defaults[key] = value
        
        logger.info(f"Auto-filled input for DRG {drg_code}: {smart_defaults}")
        return smart_defaults

# Initialize the prediction service
prediction_service = HospitalPredictionAPI()

# HTML template for the API documentation
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Hospital Prediction API</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 30px; }
        .endpoint { background-color: #f8f9fa; padding: 15px; margin: 20px 0; border-radius: 5px; }
        .method { display: inline-block; padding: 3px 8px; border-radius: 3px; color: white; font-weight: bold; }
        .get { background-color: #28a745; }
        .post { background-color: #007bff; }
        pre { background-color: #f8f9fa; padding: 15px; border-radius: 5px; overflow-x: auto; }
        .model-info { background-color: #e9ecef; padding: 10px; margin: 10px 0; border-radius: 5px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Hospital Prediction API</h1>
            <p>Machine Learning API for predicting hospital stay metrics using the NY Inpatient Discharge dataset.</p>
        </div>
        
        <h2>Available Models</h2>
        {% for model_name, metadata in models.items() %}
        <div class="model-info">
            <h3>{{ model_name }}</h3>
            <p><strong>Target:</strong> {{ metadata.get('target_column', 'Unknown') }}</p>
            <p><strong>Model Type:</strong> {{ metadata.get('model_type', 'Unknown') }}</p>
            <p><strong>RMSE:</strong> {{ metadata.get('rmse', 'Unknown') }}</p>
            <p><strong>Features:</strong> {{ metadata.get('feature_columns', []) | length }} features</p>
        </div>
        {% endfor %}
        
        <h2>API Endpoints</h2>
        
        <div class="endpoint">
            <h3><span class="method get">GET</span> /</h3>
            <p>Returns this documentation page.</p>
        </div>
        
        <div class="endpoint">
            <h3><span class="method get">GET</span> /health</h3>
            <p>Health check endpoint.</p>
            <pre>Response: {"status": "healthy", "models_loaded": number}</pre>
        </div>
        
        <div class="endpoint">
            <h3><span class="method get">GET</span> /models</h3>
            <p>List all available models.</p>
        </div>
        
        <div class="endpoint">
            <h3><span class="method post">POST</span> /predict/&lt;model_name&gt;</h3>
            <p>Make a prediction using the specified model.</p>
            <h4>Minimal Request (auto-filled):</h4>
            <pre>{
  "drg_code": 444,
  "severity_code": 1
}</pre>
            <h4>Full Request Example:</h4>
            <pre>{
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
}</pre>
            <h4>Response Example:</h4>
            <pre>{
  "model": "length_of_stay_numeric_model",
  "prediction": 3.5,
  "model_type": "RandomForest",
  "rmse": 2.1,
  "timestamp": "2025-06-19T10:30:00"
}</pre>
        </div>
        
        <div class="endpoint">
            <h3><span class="method post">POST</span> /predict/all</h3>
            <p>Make predictions using all available models.</p>
            <p>Same request format as single prediction, returns predictions from all models.</p>
        </div>
        
        <div class="endpoint">
            <h3><span class="method post">POST</span> /predict/smart</h3>
            <p>Smart prediction endpoint - only requires DRG code and severity code, auto-fills the rest.</p>
            <h4>Request:</h4>
            <pre>{
  "drg_code": 444,
  "severity_code": 1
}</pre>
        </div>
    </div>
</body>
</html>
"""

@app.route('/')
def home():
    """API documentation page"""
    return render_template_string(HTML_TEMPLATE, models=prediction_service.model_metadata)

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'models_loaded': len(prediction_service.models),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/models')
def list_models():
    """List available models"""
    models_info = {}
    for model_name, metadata in prediction_service.model_metadata.items():
        models_info[model_name] = {
            'target_column': metadata.get('target_column'),
            'model_type': metadata.get('model_type'),
            'rmse': metadata.get('rmse'),
            'feature_count': len(metadata.get('feature_columns', [])),
            'training_timestamp': metadata.get('training_timestamp')
        }
    
    return jsonify({
        'models': models_info,
        'total_models': len(models_info)
    })

@app.route('/predict/<model_name>', methods=['POST'])
def predict_single(model_name):
    """Make prediction using a specific model"""
    try:
        if not request.json:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        input_data = request.json
        result = prediction_service.predict(model_name, input_data)
        
        return jsonify({
            'success': True,
            'result': result
        })
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/predict/all', methods=['POST'])
def predict_all():
    """Make predictions using all available models"""
    try:
        if not request.json:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        input_data = request.json
        results = {}
        
        for model_name in prediction_service.models.keys():
            try:
                result = prediction_service.predict(model_name, input_data)
                results[model_name] = result
            except Exception as e:
                logger.warning(f"Failed to predict with {model_name}: {e}")
                results[model_name] = {'error': str(e)}
        
        return jsonify({
            'success': True,
            'results': results,
            'input_data': input_data
        })
        
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/predict/smart', methods=['POST'])
def predict_smart():
    """Smart prediction endpoint - only requires minimal input like DRG code and severity"""
    try:
        if not request.json:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        input_data = request.json
        
        # Validate minimal required input
        if 'drg_code' not in input_data:
            return jsonify({'error': 'drg_code is required'}), 400
        
        results = {}
        
        for model_name in prediction_service.models.keys():
            try:
                result = prediction_service.predict(model_name, input_data)
                results[model_name] = result
            except Exception as e:
                logger.warning(f"Failed to predict with {model_name}: {e}")
                results[model_name] = {'error': str(e)}
        
        return jsonify({
            'success': True,
            'results': results,
            'input_data': input_data,
            'auto_filled': True
        })
        
    except Exception as e:
        logger.error(f"Smart prediction error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/reload-models', methods=['POST'])
def reload_models():
    """Reload models from MinIO"""
    try:
        prediction_service.models = {}
        prediction_service.model_metadata = {}
        prediction_service.load_models()
        
        return jsonify({
            'success': True,
            'message': f'Reloaded {len(prediction_service.models)} models',
            'models_loaded': len(prediction_service.models)
        })
        
    except Exception as e:
        logger.error(f"Error reloading models: {e}")
        return jsonify({'error': 'Failed to reload models'}), 500

if __name__ == '__main__':
    # Run Flask app
    app.run(host='0.0.0.0', port=5001, debug=False)

