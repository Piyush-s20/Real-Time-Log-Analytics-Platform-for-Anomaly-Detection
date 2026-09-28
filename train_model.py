# train_model.py (v3)
# Trains the Isolation Forest on a realistic synthetic dataset produced by the
# same generator the streaming producer uses, and evaluates it against the
# injected-anomaly ground truth.

import argparse
import json
import os
import random
from collections import Counter

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import IsolationForest
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import confusion_matrix, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from log_producer import ANOMALY_RATE, generate_log_with_type

MODEL_PATH = os.path.join('models', 'isolation_forest_model.joblib')
METRICS_PATH = os.path.join('metrics', 'model_metrics.json')


def generate_dataset(n_logs, anomaly_rate, seed):
    """Generates n_logs logs with the producer's generator (reproducible via seed)."""
    print(f"Generating {n_logs} logs with anomaly rate {anomaly_rate}...")
    rng = random.Random(seed)
    rows = []
    for _ in range(n_logs):
        log, anomaly_type = generate_log_with_type(rng, anomaly_rate)
        log['anomaly_type'] = anomaly_type or 'normal'
        rows.append(log)
    return pd.DataFrame(rows)


def build_features(df, vectorizer, scaler, fit):
    """TF-IDF of the message + scaled request_time_ms and message_length."""
    numerical = df[['request_time_ms', 'message_length']].values
    if fit:
        message_features = vectorizer.fit_transform(df['message']).toarray()
        numerical_features = scaler.fit_transform(numerical)
    else:
        message_features = vectorizer.transform(df['message']).toarray()
        numerical_features = scaler.transform(numerical)
    return np.hstack([message_features, numerical_features])


def parse_args():
    parser = argparse.ArgumentParser(description='Train and evaluate the Isolation Forest model.')
    parser.add_argument('--n-logs', type=int, default=20000)
    parser.add_argument('--anomaly-rate', type=float, default=ANOMALY_RATE)
    parser.add_argument('--test-size', type=float, default=0.25)
    parser.add_argument('--seed', type=int, default=42)
    return parser.parse_args()


def main():
    """
    Main function to train the model, evaluate it and save it.
    """
    args = parse_args()
    df = generate_dataset(args.n_logs, args.anomaly_rate, args.seed)
    df['message_length'] = df['message'].str.len()

    train_df, test_df = train_test_split(
        df, test_size=args.test_size, random_state=args.seed, stratify=df['injected_anomaly'])
    print(f"Train: {len(train_df)} logs ({int(train_df['injected_anomaly'].sum())} anomalies), "
          f"test: {len(test_df)} logs ({int(test_df['injected_anomaly'].sum())} anomalies)")

    # --- Model Training (unsupervised: labels are only used for evaluation) ---
    # Hyperparameters were chosen on a separate validation set (different seed), not on
    # this test split. Binary TF-IDF and a large max_samples let the forest see enough of
    # the rare-but-normal templates to stop treating them as outliers.
    vectorizer = TfidfVectorizer(max_features=100, binary=True)
    scaler = StandardScaler()
    train_features = build_features(train_df, vectorizer, scaler, fit=True)

    # Contamination is set to the injected anomaly rate
    model = IsolationForest(n_estimators=200, max_samples=4096, contamination=args.anomaly_rate,
                            random_state=args.seed)
    model.fit(train_features)
    print("Model training complete.")

    # --- Evaluation on the held-out test set ---
    test_features = build_features(test_df, vectorizer, scaler, fit=False)
    y_true = test_df['injected_anomaly'].astype(int).values
    y_pred = (model.predict(test_features) == -1).astype(int)

    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    per_type_recall = {}
    for anomaly_type, group in test_df.assign(pred=y_pred).groupby('anomaly_type'):
        if anomaly_type != 'normal':
            per_type_recall[anomaly_type] = round(float(group['pred'].mean()), 4)

    metrics = {
        'precision': round(float(precision_score(y_true, y_pred, zero_division=0)), 4),
        'recall': round(float(recall_score(y_true, y_pred, zero_division=0)), 4),
        'f1': round(float(f1_score(y_true, y_pred, zero_division=0)), 4),
        'confusion_matrix': {'tn': int(tn), 'fp': int(fp), 'fn': int(fn), 'tp': int(tp)},
        'recall_by_anomaly_type': per_type_recall,
        'dataset': {
            'n_logs': args.n_logs,
            'anomaly_rate': args.anomaly_rate,
            'train_size': len(train_df),
            'test_size': len(test_df),
            'test_anomalies': int(y_true.sum()),
            'test_anomalies_by_type': dict(Counter(test_df['anomaly_type'])),
            'seed': args.seed,
        },
        'model': {
            'type': 'IsolationForest',
            'n_estimators': model.n_estimators,
            'max_samples': model.max_samples,
            'contamination': args.anomaly_rate,
            'tfidf_max_features': vectorizer.max_features,
            'tfidf_binary': vectorizer.binary,
            'n_features': int(train_features.shape[1]),
        },
        'versions': {'scikit-learn': sklearn.__version__, 'numpy': np.__version__, 'joblib': joblib.__version__},
    }

    print("\n--- Evaluation on test set (positive class = injected anomaly) ---")
    print(f"Precision: {metrics['precision']:.4f}")
    print(f"Recall:    {metrics['recall']:.4f}")
    print(f"F1:        {metrics['f1']:.4f}")
    print("Confusion matrix (rows = actual, cols = predicted):")
    print("               pred_normal  pred_anomaly")
    print(f"  normal       {tn:>11}  {fp:>12}")
    print(f"  anomaly      {fn:>11}  {tp:>12}")
    print(f"Recall by anomaly type: {per_type_recall}")

    # --- Save the Model, Transformers and Metrics ---
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(METRICS_PATH), exist_ok=True)
    model_payload = {
        'model': model,
        'vectorizer': vectorizer,
        'scaler': scaler
    }
    joblib.dump(model_payload, MODEL_PATH)
    with open(METRICS_PATH, 'w') as f:
        json.dump(metrics, f, indent=2)

    print(f"\nModel and transformers saved to '{MODEL_PATH}'")
    print(f"Metrics saved to '{METRICS_PATH}'")


if __name__ == '__main__':
    main()
