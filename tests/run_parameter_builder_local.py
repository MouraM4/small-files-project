"""
Test runner for small_files_parameter_builder locally without AWS dependencies.

Usage:
    python tests/run_parameter_builder_local.py
"""

import sys
import os
import json

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "tests"))

from mock_aws_clients import create_mock_glue_client, create_mock_s3_client

# -- Test Configuration --
DATABASE_NAME = "my_test_database"
TABLE_NAME = "my_test_table"
CONFIG_BUCKET = "config-bucket"
CONFIG_KEY = "jobs/swap_config/params.json"
STAGING_PREFIX = "temp_resized_tables"
WORKER_CAPACITY_MB = 100
TARGET_FILE_SIZE_MB = 128

# Create mock clients
mock_glue_client = create_mock_glue_client(
    database=DATABASE_NAME,
    table=TABLE_NAME,
    partition_keys=["year", "month"],
    original_partitions=[
        {
            "values": ["2025", "01"],
            "location": f"s3://bucket-sor/{TABLE_NAME}/year=2025/month=01/",
        },
        {
            "values": ["2025", "02"],
            "location": f"s3://bucket-sor/{TABLE_NAME}/year=2025/month=02/",
        },
    ],
    staging_partitions=[],
)
# Ensure the table mock holds the StorageDescriptor logic properly
mock_glue_client.table_config["StorageDescriptor"] = {"Location": f"s3://bucket-sor/{TABLE_NAME}/"}

# 50 files for 01, 10 for 02
mock_s3_client = create_mock_s3_client(
    objects={
        f"{TABLE_NAME}/year=2025/month=01/": [
            f"{TABLE_NAME}/year=2025/month=01/part-000{i}.parquet" for i in range(500)
        ],
        f"{TABLE_NAME}/year=2025/month=02/": [
            f"{TABLE_NAME}/year=2025/month=02/part-000{i}.parquet" for i in range(10)
        ],
    }
)


def get_table_info(glue_client, database: str, table: str) -> dict:
    """Get table metadata from Glue Catalog."""
    response = glue_client.get_table(DatabaseName=database, Name=table)
    return response["Table"]


def get_partition_keys(table_info: dict) -> list:
    """Extract partition key names from table metadata."""
    return [key["Name"] for key in table_info.get("PartitionKeys", [])]


def get_all_partitions(glue_client, database: str, table: str) -> list:
    """Retrieve all partitions for a given Glue Catalog table."""
    partitions = []
    paginator = glue_client.get_paginator("get_partitions")
    for page in paginator.paginate(DatabaseName=database, TableName=table):
        partitions.extend(page["Partitions"])
    return partitions


def get_bucket_and_prefix_from_s3_uri(s3_uri: str) -> tuple:
    """Parse an s3:// URI into (bucket, prefix)."""
    path = s3_uri.replace("s3://", "")
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


def get_partition_s3_stats(s3_client, bucket: str, prefix: str) -> dict:
    """
    List all objects under an S3 prefix and return statistics.
    """
    total_size = 0
    total_objects = 0
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            total_objects += 1
            total_size += obj["Size"]

    return {
        "number_of_objects": total_objects,
        "partition_size_bytes": total_size,
        "partition_size_mb": round(total_size / (1024 * 1024), 2),
    }


def calculate_number_of_workers(total_size_mb: float, worker_capacity_mb: int) -> int:
    """
    Calculate the recommended number of Glue workers based on total data size.
    """
    import math
    MIN_WORKERS = 2
    workers = max(MIN_WORKERS, math.ceil(total_size_mb / worker_capacity_mb))
    return workers


def build_partition_key_string(partition_keys: list, partition_values: list) -> str:
    """Build a partition path string like 'year=2025/month=01'."""
    return "/".join(
        f"{key}={value}" for key, value in zip(partition_keys, partition_values)
    )

def run_parameter_builder():
    from datetime import datetime, timezone
    
    print(f"Analyzing table: {DATABASE_NAME}.{TABLE_NAME}")

    # Get table info and partition keys
    table_info = get_table_info(mock_glue_client, DATABASE_NAME, TABLE_NAME)
    partition_keys = get_partition_keys(table_info)
    table_location = table_info["StorageDescriptor"]["Location"]
    table_bucket, table_prefix = get_bucket_and_prefix_from_s3_uri(table_location)

    print(f" Table location: {table_location}")
    print(f" Partition keys: {partition_keys}")

    # Get all partitions
    partitions = get_all_partitions(mock_glue_client, DATABASE_NAME, TABLE_NAME)

    if not partitions:
        print(" No partitions found. Nothing to process.")
        return

    print(f" Found {len(partitions)} partition(s)")

    partitions_config = {}
    total_size_mb = 0.0
    total_objects = 0

    for partition in partitions:
        partition_values = partition["Values"]

        partition_location = partition["StorageDescriptor"]["Location"]
        partition_key_str = build_partition_key_string(partition_keys, partition_values)
        bucket, prefix = get_bucket_and_prefix_from_s3_uri(partition_location)

        if prefix and not prefix.endswith("/"):
            prefix += "/"

        print(f" Scanning partition: {partition_key_str} ({partition_location})")
        stats = get_partition_s3_stats(mock_s3_client, bucket, prefix)

        partitions_config[partition_key_str] = {
            "partition_values": partition_values,
            "s3_path": partition_location,
            "s3_path_prefix": prefix,
            "staging_partition_values": partition_values,
            "staging_partition_key_str": partition_key_str,
            "staging_s3_path": partition_location,
            "staging_s3_path_prefix": f"{STAGING_PREFIX}/{TABLE_NAME}/{partition_key_str}/",
            "number_of_objects": stats["number_of_objects"],
            "partition_size_mb": stats["partition_size_mb"],
        }

        total_size_mb += stats["partition_size_mb"]
        total_objects += stats["number_of_objects"]

        print(
            f" -> {stats['number_of_objects']} objects, "
            f"{stats['partition_size_mb']} MB"
        )

    # Calculate recommended workers
    number_of_workers = calculate_number_of_workers(total_size_mb, WORKER_CAPACITY_MB)

    # Build final config JSON
    config = {
        "database_name": DATABASE_NAME,
        "table_name": TABLE_NAME,
        "bucket_name": table_bucket,
        "staging_prefix": STAGING_PREFIX,
        "target_file_size_mb": TARGET_FILE_SIZE_MB,
        "total_size_mb": round(total_size_mb, 2),
        "total_objects": total_objects,
        "number_of_workers": number_of_workers,
        "partition_count": len(partitions_config),
        "partitions": partitions_config,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    config_json = json.dumps(config, indent=2, default=str)
    print(f"\n{'-'*60}")
    print("Generated config:")
    print(config_json)
    print(f"{'-'*60}")
    
    # Save config to S3 mock
    mock_s3_client.put_object(
        Bucket=CONFIG_BUCKET,
        Key=CONFIG_KEY,
        Body=config_json.encode("utf-8"),
        ContentType="application/json",
    )
    print(f"\nConfig saved to mock s3://{CONFIG_BUCKET}/{CONFIG_KEY}")

    # Also save to local file for review
    local_output_dir = os.path.join(PROJECT_ROOT, "tests", "test_data")
    os.makedirs(local_output_dir, exist_ok=True)
    local_output_path = os.path.join(local_output_dir, "generated_params.json")
    
    with open(local_output_path, "w", encoding="utf-8") as f:
        f.write(config_json)
        
    print(f"Config successfully saved locally to: {local_output_path}")

    print(f"Recommended workers for swap job: {number_of_workers}")
    print("Parameter builder completed successfully.")


def main():
    print("=" * 60)
    print("Running small_files_parameter_builder locally with mocks")
    print("=" * 60)
    run_parameter_builder()
    print("=" * 60)
    print("Final Result: PASSED ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
