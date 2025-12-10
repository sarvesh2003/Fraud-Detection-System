# Dockerfile
FROM apache/airflow:3.0.3

RUN pip install --upgrade pip && pip install \
    pandas \
    scikit-learn \
    joblib \
    mlflow==2.22.1 \
    dagshub \
    python-dotenv \
    dvc

RUN python -c "import pandas, sklearn, joblib, mlflow, dagshub, dotenv, dvc"