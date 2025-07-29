# Dockerfile
FROM apache/airflow:3.0.3

# Upgrade pip and install project dependencies
RUN pip install --upgrade pip && pip install \
    pandas \
    scikit-learn \
    joblib \
    mlflow==2.22.1 \
    dagshub \
    python-dotenv \
    dvc

# Optional: check installations
RUN python -c "import pandas, sklearn, joblib, mlflow, dagshub, dotenv, dvc"