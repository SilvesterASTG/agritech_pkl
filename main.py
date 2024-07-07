from flask import Flask, request, jsonify
import joblib
import numpy as np
import os
import logging
import geopandas as gpd
from shapely.geometry import Point
import pymysql
import pandas as pd

app = Flask(__name__)

# Configure logging (optional but recommended)
logging.basicConfig(level=logging.DEBUG)

# Load the model and other necessary files (with potential error handling)
try:
    model = joblib.load("svrold-new.pkl")
    X_names = joblib.load("X_names.pkl")
    xscaler = joblib.load("xscaler.pkl")
    yscaler = joblib.load("yscaler.pkl")
    
    # Load GeoJSON file
    geojson_file = 'map.geojson'
    gdf = gpd.read_file(geojson_file)
   
except FileNotFoundError as e:
    logging.error("Error loading model or scaler files:", e)
    # Handle error (e.g., return HTTP error code or informative message)

@app.route("/")
def hello_world():
    """Example Hello World route."""
    name = os.environ.get("NAME", "World")
    return f"Hello {name}!"

@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Get JSON input data
        data = request.get_json()

        # Validate input data structure (example)
        if not all(key in data for key in X_names):
            return jsonify({'error': 'Missing required features in input data'}), 400

        # Extract features from JSON data
        features = [data[name] for name in X_names]

        # Convert features to numpy array
        features_array = np.array([features])

        # Scale features
        scaled_features = xscaler.transform(features_array)

        # Make prediction
        prediction_scaled = model.predict(scaled_features)

        # Inverse scaling for prediction
        prediction = yscaler.inverse_transform(prediction_scaled.reshape(-1, 1))[0][0]

        # Return prediction as JSON response
        return jsonify({'prediction': round(prediction, 2)})
    except Exception as e:  # Catch generic exceptions for broader error handling
        logging.error("Error during prediction:", e)
        # Handle error (e.g., return HTTP error code or informative message)
        
@app.route('/get-location', methods=['POST'])
def get_location():
    try:
        # Parse JSON request
        data = request.get_json()
        lat = data['lat']
        lon = data['lon']
        
        # Create Point from lat/lon
        point = Point(lon, lat)
        
        # Find the location containing the point
        location_name = None
        for idx, row in gdf.iterrows():
            if row['geometry'].contains(point):
                location_name = row['name']  # Assume the GeoJSON has a property 'name'
                break
        
        if location_name:
            return jsonify({'location': location_name}), 200
        else:
            return jsonify({'error': 'Location not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
