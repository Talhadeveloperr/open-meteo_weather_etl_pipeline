# Test S3 Connection Script
import boto3
import yaml

# Load AWS credentials
with open("config/aws_config.yaml", "r") as f:
    config = yaml.safe_load(f)

aws_conf = config["aws"]
s3_conf = config["s3"]

# Create S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_conf["access_key_id"],
    aws_secret_access_key=aws_conf["secret_access_key"],
    region_name=aws_conf["region_name"],
)

# Test upload
test_file_content = b"Hello from Talha's Airflow ETL project!"
test_file_name = "test_upload.txt"

try:
    s3.put_object(
        Bucket=s3_conf["bucket_name"],
        Key=f"{s3_conf['raw_prefix']}{test_file_name}",
        Body=test_file_content,
    )
    print(f"✅ Test file uploaded successfully to s3://{s3_conf['bucket_name']}/{s3_conf['raw_prefix']}{test_file_name}")

    # Optional: Verify listing
    response = s3.list_objects_v2(Bucket=s3_conf["bucket_name"], Prefix=s3_conf["raw_prefix"])
    print("Files in RAW folder:")
    for obj in response.get("Contents", []):
        print("-", obj["Key"])

except Exception as e:
    print("❌ Error connecting to S3:", e)
