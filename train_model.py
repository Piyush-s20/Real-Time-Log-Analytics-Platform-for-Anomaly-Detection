# train_model.py (v2)
# This script now includes message length as a feature for the model.

import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
import joblib
import numpy as np

def generate_sample_data():
    """
    Generates a sample pandas DataFrame of log data for training.
    Includes both normal and anomalous log entries.
    """
    print("Generating sample log data...")
    data = {
        'level': [
            'INFO', 'INFO', 'INFO', 'WARNING', 'INFO', 'ERROR', 'INFO', 'DEBUG',
            'CRITICAL', 'INFO', 'INFO', 'ERROR', 'INFO', 'WARNING'
        ],
        'message': [
            'User logged in successfully',
            'Data processed for request A',
            'User logged out',
            'Low disk space warning',
            'Payment processed for order #1234',
            'Failed to connect to database: Connection refused',
            'API call successful',
            'Debugging variable x=10',
            'System shutting down due to critical error that has a very long message to simulate a stack trace',
            'User viewed page /home',
            'Data processed for request B',
            'NullPointerException at com.example.Service:123',
            'Cache cleared successfully',
            'Request timed out after 3000ms'
        ],
        'request_time_ms': [
            120, 80, 50, 200, 150, 5000, 90, 20,
            10000, 60, 85, 4500, 40, 3000
        ]
    }
    return pd.DataFrame(data)

def main():
    """
    Main function to train the model and save it.
    """
    df = generate_sample_data()

    # --- Feature Engineering ---
    # NEW: Add message length as a feature.
    df['message_length'] = df['message'].apply(len)
    
    print("Preparing data and training the Isolation Forest model with new 'message_length' feature...")

    vectorizer = TfidfVectorizer(max_features=100)
    
    # Scale both numerical features together
    numerical_features = df[['request_time_ms', 'message_length']].values
    scaler = StandardScaler()
    scaled_numerical_features = scaler.fit_transform(numerical_features)

    message_features = vectorizer.fit_transform(df['message']).toarray()

    # Combine all features
    features = np.hstack([message_features, scaled_numerical_features])

    # --- Model Training ---
    # You can experiment with this 'contamination' value (e.g., 0.1 for 10%)
    model = IsolationForest(n_estimators=100, contamination=0.2, random_state=42)
    model.fit(features)

    print("Model training complete.")

    # --- Save the Model and Transformers ---
    model_payload = {
        'model': model,
        'vectorizer': vectorizer,
        'scaler': scaler
    }
    
    model_filename = 'isolation_forest_model.joblib'
    joblib.dump(model_payload, model_filename)

    print(f"Model and transformers saved to '{model_filename}'")
    print("\n--- Testing the model on sample data ---")
    
    predictions = model.predict(features)
    df['anomaly_score'] = model.decision_function(features)
    df['is_anomaly'] = predictions

    print(df[['message', 'message_length', 'request_time_ms', 'is_anomaly', 'anomaly_score']])


if __name__ == '__main__':
    main()
