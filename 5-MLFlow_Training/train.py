import pandas as pd
import mlflow
import dagshub
from dagshub import init
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score
import joblib
from dotenv import load_dotenv
import os

load_dotenv()
username = os.getenv("DAGSHUB_USERNAME")
token = os.getenv("DAGSHUB_TOKEN")
print("Username:", username)
print("Token:", token)

dagshub.auth.add_app_token(token=token)

init(repo_owner='sarveshchezhian2003', repo_name='Fraud-Detection-System', mlflow=True)

# Load and preprocess data
df = pd.read_csv("/mnt/project/4-Data/dataset.csv")
df["type_encoded"] = df["type"].astype("category").cat.codes
df["delta_orig"] = df["newbalanceOrig"] - df["oldbalanceOrg"]
df["delta_dest"] = df["newbalanceDest"] - df["oldbalanceDest"]

# Prepare features
features = ["amount", "type_encoded", "delta_orig", "delta_dest"]
X = df[features]
y = df["isFraud"]

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, random_state=42)
print("Train Test Split Done")

# MLflow experiment
with mlflow.start_run():
    print("Starting to fit the classifier")
    
    # Model parameters
    n_estimators = 2
    max_depth = 2
    
    model = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth)
    model.fit(X_train, y_train)
    print("Fitting done")
    
    # Predictions and metrics
    preds = model.predict(X_test)
    print("Calculating accuracy and auc")
    acc = accuracy_score(y_test, preds)
    auc = roc_auc_score(y_test, preds)
    
    # Log parameters
    mlflow.log_param("n_estimators", n_estimators)
    mlflow.log_param("max_depth", max_depth)
    
    # Log metrics
    mlflow.log_metric("accuracy", acc)
    mlflow.log_metric("auc", auc)
    
    print(f"Accuracy: {acc:.4f}")
    print(f"AUC: {auc:.4f}")
    
    # Save model locally
    joblib.dump(model, "model.joblib")
    
    # Log model file to MLflow
    mlflow.log_artifact("model.joblib")
    
    print("Model logged successfully!")