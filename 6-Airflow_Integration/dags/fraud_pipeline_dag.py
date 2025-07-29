from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
import subprocess

logger = LoggingMixin().log

@task()
def pull_data():
    """Pull data using DVC"""
    logger.info("Starting DVC pull task...")
    logger.info(f"Current working directory: {subprocess.run(['pwd'], capture_output=True, text=True).stdout.strip()}")
    
    # Check if DVC is available
    try:
        dvc_version = subprocess.run(["dvc", "--version"], capture_output=True, text=True, timeout=30)
        logger.info(f"DVC version: {dvc_version.stdout.strip()}")
    except Exception as e:
        logger.error(f"DVC not found or not accessible: {e}")
        raise
    
    # Check if target directory exists
    import os
    # target_dir = "/opt/airflow/4-Data"
    target_dir = "/mnt/project/4-Data"
    if not os.path.exists(target_dir):
        logger.error(f"Target directory does not exist: {target_dir}")
        raise FileNotFoundError(f"Directory {target_dir} not found")
    
    logger.info(f"Changing to directory: {target_dir}")
    
    try:
        result = subprocess.run(
            ["dvc", "pull", "--force"], 
            cwd=target_dir, 
            check=True, 
            capture_output=True, 
            text=True,
            timeout=300  # 5 minute timeout
        )
        logger.info(f"DVC pull successful: {result.stdout}")
        if result.stderr:
            logger.warning(f"DVC pull stderr: {result.stderr}")
        return "Data pulled successfully"
    except subprocess.TimeoutExpired:
        logger.error("DVC pull timed out after 5 minutes")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"DVC pull failed with return code {e.returncode}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise

@task()
def run_training():
    """Run ML model training"""
    logger.info("Starting training task...")
    
    # Check if target directory exists
    import os
    # target_dir = "/opt/airflow/5-MLFlow_Training"
    target_dir = "/mnt/project/5-MLFlow_Training"
    if not os.path.exists(target_dir):
        logger.error(f"Target directory does not exist: {target_dir}")
        raise FileNotFoundError(f"Directory {target_dir} not found")
    
    # Check if train.py exists
    train_script = os.path.join(target_dir, "train.py")
    if not os.path.exists(train_script):
        logger.error(f"Training script not found: {train_script}")
        raise FileNotFoundError(f"Training script {train_script} not found")
    
    logger.info(f"Running training in directory: {target_dir}")
    
    try:
        result = subprocess.run(
            ["python3", "train.py"], 
            cwd=target_dir, 
            check=True, 
            capture_output=True, 
            text=True,
            timeout=1800  # 30 minute timeout
        )
        logger.info(f"Training successful: {result.stdout}")
        if result.stderr:
            logger.warning(f"Training stderr: {result.stderr}")
        return "Training completed successfully"
    except subprocess.TimeoutExpired:
        logger.error("Training timed out after 30 minutes")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"Training failed with return code {e.returncode}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise

@task()
def push_data():
    """Push updated DVC-tracked data (like model.joblib) to remote"""
    logger.info("Starting DVC push task...")
    import os

    # Same directory as training
    target_dir = "/mnt/project/5-MLFlow_Training"
    if not os.path.exists(target_dir):
        logger.error(f"Target directory does not exist: {target_dir}")
        raise FileNotFoundError(f"Directory {target_dir} not found")

    try:
        result = subprocess.run(
            ["dvc", "push"],
            cwd=target_dir,
            check=True,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes
        )
        logger.info(f"DVC push successful: {result.stdout}")
        if result.stderr:
            logger.warning(f"DVC push stderr: {result.stderr}")
        return "Data pushed successfully"
    except subprocess.TimeoutExpired:
        logger.error("DVC push timed out")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"DVC push failed: return code {e.returncode}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise


@dag(
    dag_id="fraud_detection_pipeline",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),  # Use datetime instead of days_ago
    catchup=False,
    tags=["fraud", "mlflow", "dvc"],
    doc_md="""
    # Fraud Detection Pipeline
    
    This DAG runs a daily fraud detection pipeline that:
    1. Pulls latest data using DVC
    2. Trains ML model using MLFlow
    """,
    default_args={
        'owner': 'data-team',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=60),  # Kill task if it runs longer than 60 minutes
        'sla': timedelta(hours=2),  # SLA for the tasks
    }
)
def fraud_pipeline():
    """Define the fraud detection pipeline"""
    data_task = pull_data()
    training_task = run_training()
    pushing_data = push_data()
    
    # Set task dependencies
    data_task >> training_task >> pushing_data

# Instantiate the DAG
fraud_dag = fraud_pipeline()