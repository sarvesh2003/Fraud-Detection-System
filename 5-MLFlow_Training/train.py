import pandas as pd
import mlflow
from dagshub import init
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score
import joblib
from dotenv import load_dotenv
import os

# Load the environment variables from the .env file
load_dotenv()

# Now use the environment variables
username = os.getenv("DAGSHUB_USERNAME")
token = os.getenv("DAGSHUB_TOKEN")

print("Username:", username)
print("Token:", token)

# ✅ Connect to DAGsHub
init(repo_owner='sarveshchezhian2003', repo_name='Fraud-Detection-System', mlflow=True)


# # ✅ Enable MLflow autolog (optional)
# mlflow.autolog()

df = pd.read_csv("../4-Data/dataset.csv")

# Example feature engineering
df["type_encoded"] = df["type"].astype("category").cat.codes
df["delta_orig"] = df["newbalanceOrig"] - df["oldbalanceOrg"]
df["delta_dest"] = df["newbalanceDest"] - df["oldbalanceDest"]

features = ["amount", "type_encoded", "delta_orig", "delta_dest"]
X = df[features]
y = df["isFraud"]

X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, random_state=42)
print("Train Test Split Done")
with mlflow.start_run():
    print("Starting to fit the classifier")
    # model = RandomForestClassifier(n_estimators=100, max_depth=6)
    model = RandomForestClassifier(n_estimators=2, max_depth=2)
    model.fit(X_train, y_train)
    print("Fitting done")
    preds = model.predict(X_test)
    print("Waiting for accuracy and auc")
    acc = accuracy_score(y_test, preds)
    auc = roc_auc_score(y_test, preds)

    # Explicit logging (redundant if autolog is on)
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 6)
    mlflow.log_metric("accuracy", acc)
    mlflow.log_metric("auc", auc)

    # Save model locally
    joblib.dump(model, "model.joblib")

    # Log model file to MLflow (optional)
    mlflow.log_artifact("model.joblib")
