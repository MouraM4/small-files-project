import sys
import json
import math
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions

# Parse job arguments
optional_args = ["TARGET_FILE_SIZE_MB", "WORKER_CAPACITY_MB", "RESIZE_NEEDED", "TARGET_PARTITION"]
required_args = ["JOB_NAME", "DATABASE_NAME", "TABLE_NAME", "CONFIG_BUCKET", "CONFIG_KEY", "STAGING_PREFIX"]
args_to_resolve = required_args + [arg for arg in optional_args if f"--{arg}" in sys.argv]

args = getResolvedOptions(sys.argv, args_to_resolve)

DATABASE_NAME = args["DATABASE_NAME"]
TABLE_NAME = args["TABLE_NAME"]
CONFIG_BUCKET = args["CONFIG_BUCKET"]
CONFIG_KEY = args["CONFIG_KEY"]
STAGING_PREFIX = args["STAGING_PREFIX"].rstrip("/")
TARGET_FILE_SIZE_MB = int(args.get("TARGET_FILE_SIZE_MB", "128"))
WORKER_CAPACITY_MB = int(args.get("WORKER_CAPACITY_MB", "6144"))
RESIZE_NEEDED = args.get("RESIZE_NEEDED", "true").lower() == "true"
TARGET_PARTITION = args.get("TARGET_PARTITION")

# Minimum number of workers for the swap job
MIN_WORKERS = 2

glue_client = boto3.client("glue")
s3_client = boto3.client("s3")


# -- Helper functions --


def get_table_info(database: str, table: str) -> dict:
    """Get table metadata from Glue Catalog."""
    response = glue_client.get_table(DatabaseName=database, Name=table)
    return response["Table"]


def get_partition_keys(table_info: dict) -> list:
    """Extract partition key names from table metadata."""
    return [key["Name"] for key in table_info.get("PartitionKeys", [])]


def get_all_partitions(database: str, table: str) -> list:
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


def get_partition_s3_stats(bucket: str, prefix: str) -> dict:
    """
    List all objects under an S3 prefix and return:
     - number_of_objects: total file count
     - partition_size_bytes: total size in bytes
     - partition_size_mb: total size in MB (rounded to 2 decimals)
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
    Each worker can process approximately `worker_capacity_mb` of data.
    """
    workers = max(MIN_WORKERS, math.ceil(total_size_mb / worker_capacity_mb))
    return workers


def build_partition_key_string(partition_keys: list, partition_values: list) -> str:
    """Build a partition path string like 'year=2025/month=01'."""
    return "/".join(
        f"{key}={value}" for key, value in zip(partition_keys, partition_values)
    )


# -- Main logic --


def main():
    print(f"Analyzing table: {DATABASE_NAME}.{TABLE_NAME}")

    # Get table info and partition keys
    table_info = get_table_info(DATABASE_NAME, TABLE_NAME)
    partition_keys = get_partition_keys(table_info)
    table_location = table_info["StorageDescriptor"]["Location"]
    table_bucket, table_prefix = get_bucket_and_prefix_from_s3_uri(table_location)

    print(f" Table location: {table_location}")
    print(f" Partition keys: {partition_keys}")

    # Get all partitions
    partitions = get_all_partitions(DATABASE_NAME, TABLE_NAME)

    if not partitions:
        print(" No partitions found. Nothing to process.")
        return

    print(f" Found {len(partitions)} partition(s)")

    # Analyze each partition (skip partitions that are already staging)
    partitions_config = []
    total_size_mb = 0.0
    total_objects = 0

    for partition in partitions:
        partition_values = partition["Values"]

        partition_location = partition["StorageDescriptor"]["Location"]
        partition_key_str = build_partition_key_string(partition_keys, partition_values)
        bucket, prefix = get_bucket_and_prefix_from_s3_uri(partition_location)

        if TARGET_PARTITION and TARGET_PARTITION not in partition_location and TARGET_PARTITION not in partition_key_str:
            print(f" Skipping partition: {partition_key_str} (does not match TARGET_PARTITION: {TARGET_PARTITION})")
            continue

        # Ensure prefix ends with / for proper listing
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        print(f" Scanning partition: {partition_key_str} ({partition_location})")
        stats = get_partition_s3_stats(bucket, prefix)
        
        if stats["number_of_objects"] <= 1:
            print(f" -> {stats['number_of_objects']} objects. Skipping (no small files issue).")
            continue

        avg_size = round(stats["partition_size_mb"] / stats["number_of_objects"], 4) if stats["number_of_objects"] > 0 else 0

        partitions_config.append({
            "partition_values": partition_values,
            "s3_path": partition_location,
            "s3_path_prefix": prefix,
            "staging_partition_values": partition_values,
            "staging_partition_key_str": partition_key_str,
            "staging_s3_path": f"s3://{table_bucket}/{STAGING_PREFIX}/{TABLE_NAME}/{partition_key_str}/",
            "staging_s3_path_prefix": f"{STAGING_PREFIX}/{TABLE_NAME}/{partition_key_str}/",
            "number_of_objects": stats["number_of_objects"],
            "partition_size_mb": stats["partition_size_mb"],
            "average_partition_size_mb": avg_size,
            "resize_needed": RESIZE_NEEDED,
        })

        total_size_mb += stats["partition_size_mb"]
        total_objects += stats["number_of_objects"]

        print(
            f" -> {stats['number_of_objects']} objects, "
            f"{stats['partition_size_mb']} MB"
        )
        # print(
        #     f"-> Staging partition: {staging_partition_key_str}"
        # )

    object_key = f"{CONFIG_KEY}/{TABLE_NAME}/config.json"
    
    # Try to load existing config to append new partitions
    existing_config = None
    try:
        response = s3_client.get_object(Bucket=CONFIG_BUCKET, Key=object_key)
        existing_data = response["Body"].read().decode("utf-8")
        existing_config = json.loads(existing_data)
        print(f" Found existing config for {TABLE_NAME}. Appending new configurations.")
    except Exception as e:
        print(f" No valid existing config found for {TABLE_NAME} (or could not be read). Starting fresh.")

    if existing_config and "partitions" in existing_config:
        # Merge new configurations with existing ones
        existing_partitions = existing_config.get("partitions", [])
        
        # Optionally remove older entries for the same partitions
        new_partition_keys = {p["staging_partition_key_str"] for p in partitions_config}
        merged_partitions = [p for p in existing_partitions if p["staging_partition_key_str"] not in new_partition_keys]
        merged_partitions.extend(partitions_config)
        
        # Calculate updated metrics
        total_size_mb = sum(p["partition_size_mb"] for p in merged_partitions)
        total_objects = sum(p["number_of_objects"] for p in merged_partitions)
        number_of_workers_merged = calculate_number_of_workers(total_size_mb, WORKER_CAPACITY_MB)
        
        config = {
            "database_name": DATABASE_NAME,
            "table_name": TABLE_NAME,
            "bucket_name": table_bucket,
            "staging_prefix": STAGING_PREFIX,
            "target_file_size_mb": TARGET_FILE_SIZE_MB,
            "total_size_mb": round(total_size_mb, 2),
            "total_objects": total_objects,
            "number_of_workers": number_of_workers_merged,
            "partition_count": len(merged_partitions),
            "partitions": merged_partitions,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        number_of_workers = number_of_workers_merged
    else:
        # Calculate recommended workers for a fresh config
        number_of_workers = calculate_number_of_workers(total_size_mb, WORKER_CAPACITY_MB)

        # Build fresh config JSON
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

    # Save config to S3
    s3_client.put_object(
        Bucket=CONFIG_BUCKET,
        Key=object_key,
        Body=config_json.encode("utf-8"),
        ContentType="application/json",
    )

    print(f"\nConfig saved to s3://{CONFIG_BUCKET}/{CONFIG_KEY}/{TABLE_NAME}/config.json")
    print(f"Recommended workers for swap job: {number_of_workers}")
    print("Parameter builder completed successfully.")


if __name__ == "__main__":
    main()
