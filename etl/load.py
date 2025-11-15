# etl/load.py
import os
import boto3
import yaml
import pathlib
from datetime import datetime
from botocore.exceptions import BotoCoreError, NoCredentialsError, ClientError
from etl.extract import log_etl_status  # reusing existing ETL logger

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
BASE_DIR = str(pathlib.Path(__file__).resolve().parents[1])
CONFIG_PATH = os.path.join(BASE_DIR, "config", "aws_config.yaml")  # ✅ use .yaml
DEFAULT_CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")


# ---------------------------------------------------------------------
# Config / S3 helpers
# ---------------------------------------------------------------------
def _read_config(path: str = CONFIG_PATH) -> dict:
    """Read YAML config file, with fallback path support."""
    if not os.path.exists(path):
        # fallback for Airflow DAG relative path
        alt_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config", "aws_config.yaml"))
        if os.path.exists(alt_path):
            path = alt_path
        else:
            raise FileNotFoundError(f"❌ Config file not found at {path} or {alt_path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise RuntimeError(f"❌ Error reading YAML config: {e}") from e


def _make_s3_client(cfg: dict):
    aws_cfg = cfg.get("aws", {})
    region_name = aws_cfg.get("region_name")
    access_key = aws_cfg.get("access_key_id")
    secret_key = aws_cfg.get("secret_access_key")

    if not all([region_name, access_key, secret_key]):
        raise ValueError("❌ Incomplete AWS configuration in aws_config.yaml")

    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name,
    )


def _upload_one_file(s3_client, bucket: str, prefix: str, local_path: str) -> str:
    """Upload a single CSV and return its S3 URI."""
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Clean file not found: {local_path}")

    timestamp = datetime.now().strftime("%Y/%m/%d_%H%M%S")
    file_name = os.path.basename(local_path)
    key = f"{prefix}{timestamp}_{file_name}"

    s3_client.upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"


# ---------------------------------------------------------------------
# Main Public Function
# ---------------------------------------------------------------------
def upload_to_s3(clean_path: str, **kwargs):
    """
    Upload a cleaned CSV file or directory of CSVs to S3.
    """
    cfg = _read_config()
    s3_cfg = cfg.get("s3", {})
    bucket = s3_cfg.get("bucket_name")
    prefix = s3_cfg.get("clean_prefix", "clean/")

    if not bucket:
        raise ValueError("❌ 'bucket_name' missing in aws_config.yaml under 's3'")

    s3_client = _make_s3_client(cfg)

    if not os.path.exists(clean_path):
        msg = f"❌ Path does not exist: {clean_path}"
        log_etl_status("", clean_path, "FAILED", msg)
        raise FileNotFoundError(msg)

    # Collect all CSV files
    paths = []
    if os.path.isdir(clean_path):
        for name in os.listdir(clean_path):
            if name.lower().endswith(".csv"):
                paths.append(os.path.join(clean_path, name))
    else:
        if clean_path.lower().endswith(".csv"):
            paths = [clean_path]
        else:
            msg = f"❌ Expected a .csv file, got: {clean_path}"
            log_etl_status("", clean_path, "FAILED", msg)
            raise ValueError(msg)

    if not paths:
        msg = f"No CSV files found to upload in: {clean_path}"
        log_etl_status("", clean_path, "FAILED", msg)
        raise FileNotFoundError(msg)

    uploaded = []
    for p in paths:
        try:
            uri = _upload_one_file(s3_client, bucket, prefix, p)
            print(f"✅ Uploaded to S3: {uri}")
            log_etl_status("", p, "SUCCESS", "")
            uploaded.append(uri)
        except (FileNotFoundError, NoCredentialsError, BotoCoreError, ClientError) as e:
            msg = f"S3 upload failed for {p}: {e}"
            print(f"❌ {msg}")
            log_etl_status("", p, "FAILED", msg)
            raise

    print(f"✅ Uploaded {len(uploaded)} file(s) to S3.")
    return uploaded


def load_data(clean_dir: str = DEFAULT_CLEAN_DIR):
    """Convenience function for local testing."""
    return upload_to_s3(clean_dir)


if __name__ == "__main__":
    load_data()
