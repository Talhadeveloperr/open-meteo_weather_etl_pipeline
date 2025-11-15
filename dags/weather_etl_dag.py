import os
import sys
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------
# ✅ PROJECT PATH CONFIGURATION (important for imports in Airflow)
# ---------------------------------------------------------------------
PROJECT_PATH = "/mnt/d/projects/open-meteo_weather_etl_pipeline"
if PROJECT_PATH not in sys.path:
    sys.path.insert(0, PROJECT_PATH)

# ---------------------------------------------------------------------
# ✅ IMPORT ETL MODULES
# ---------------------------------------------------------------------
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import upload_to_s3

# ---------------------------------------------------------------------
# ✅ LOG FILE CONFIGURATION
# ---------------------------------------------------------------------
LOG_FILE = os.path.join(PROJECT_PATH, "logs", "etl_error_log.csv")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# Ensure header exists in log file
if not os.path.exists(LOG_FILE) or os.stat(LOG_FILE).st_size == 0:
    with open(LOG_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "raw_file", "clean_file", "status", "error_message"])


def log_etl_status(raw_file, clean_file, status, error_message=""):
    """
    Logs ETL task execution status to etl_error_log.csv
    """
    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            raw_file or "",
            clean_file or "",
            status,
            error_message
        ])

# ---------------------------------------------------------------------
# ✅ TASK FUNCTIONS
# ---------------------------------------------------------------------
def run_extract(**context):
    try:
        raw_file = extract_data()  # Must return full raw file path
        if not raw_file or not os.path.exists(raw_file):
            raise ValueError("Extract task did not return a valid raw file path.")
        context["ti"].xcom_push(key="raw_file", value=raw_file)
        log_etl_status(raw_file, "", "SUCCESS")
        print(f"[Extract] Completed successfully: {raw_file}")
    except Exception as e:
        log_etl_status("", "", "FAILED", str(e))
        print(f"[Extract] Failed: {e}")
        raise


def run_transform(**context):
    try:
        raw_file = context["ti"].xcom_pull(task_ids="extract_weather_data", key="raw_file")
        if not raw_file or not os.path.exists(raw_file):
            raise ValueError("No raw_file path received from extract task.")
        clean_file = transform_data(raw_file)
        if not clean_file or not os.path.exists(clean_file):
            raise ValueError("Transform task did not return a valid clean file path.")
        context["ti"].xcom_push(key="clean_file", value=clean_file)
        log_etl_status(raw_file, clean_file, "SUCCESS")
        print(f"[Transform] Completed successfully: {clean_file}")
    except Exception as e:
        raw_file = context["ti"].xcom_pull(task_ids="extract_weather_data", key="raw_file")
        log_etl_status(raw_file, "", "FAILED", str(e))
        print(f"[Transform] Failed: {e}")
        raise


def run_load(**context):
    """
    Airflow task: Upload cleaned file(s) to S3.
    """
    try:
        raw_file = context["ti"].xcom_pull(task_ids="extract_weather_data", key="raw_file")
        clean_file = context["ti"].xcom_pull(task_ids="transform_weather_data", key="clean_file")

        if not clean_file or not os.path.exists(clean_file):
            raise ValueError(f"No valid clean_file found: {clean_file}")

        print(f"[Load] Uploading clean data from {clean_file} to S3...")
        s3_uris = upload_to_s3(clean_file)  # ✅ pass only one argument
        log_etl_status(raw_file, clean_file, "SUCCESS", "")
        print(f"[Load] Upload successful. Uploaded URIs: {s3_uris}")

        context["ti"].xcom_push(key="s3_uris", value=s3_uris)
    except Exception as e:
        raw_file = context["ti"].xcom_pull(task_ids="extract_weather_data", key="raw_file")
        clean_file = context["ti"].xcom_pull(task_ids="transform_weather_data", key="clean_file")
        log_etl_status(raw_file or "", clean_file or "", "FAILED", str(e))
        print(f"[Load] Failed: {e}")
        raise


# ---------------------------------------------------------------------
# ✅ DAG DEFAULT ARGUMENTS
# ---------------------------------------------------------------------
default_args = {
    "owner": "talha",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# ---------------------------------------------------------------------
# ✅ DAG DEFINITION
# ---------------------------------------------------------------------
with DAG(
    dag_id="weather_etl_dag",
    description="Hourly ETL pipeline for Open-Meteo weather data",
    default_args=default_args,
    start_date=datetime(2025, 11, 5),
    schedule=timedelta(hours=1),
    catchup=False,
    tags=["weather", "open-meteo"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=run_extract
    )

    transform_task = PythonOperator(
        task_id="transform_weather_data",
        python_callable=run_transform
    )

    load_task = PythonOperator(
        task_id="load_to_s3",
        python_callable=run_load
    )

    # Task dependency chain
    extract_task >> transform_task >> load_task
