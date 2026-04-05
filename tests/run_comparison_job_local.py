"""
Test runner for small_files_comparison_job with mocked AWS clients and Spark.

This script allows debugging the comparison job locally without AWS dependencies.
Run this file directly to test the job, or use it with VS Code debugger.

Usage:
    python tests/run_comparison_job_local.py
"""

import sys
import os

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "tests"))

from mock_aws_clients import create_mock_glue_client, create_mock_s3_client

# Test configuration
TEST_DATA_PATH = os.path.join(PROJECT_ROOT, "tests", "test_data")
DATABASE_NAME = "my_test_database"
TABLE_NAME = "my_test_table"
BUCKET_NAME = "bucket-sor"
ORIGINAL_PREFIX = f"{TABLE_NAME}"
HOURS_THRESHOLD = 3

# Local file paths (simulating S3 locations)
# Original: s3://bucket-sor/my_test_table/year=2025/month=01/
# Staging: s3://bucket-sor/temp_resized_tables/my_test_table/year=2025/month=01/
ORIGINAL_LOCATION = os.path.join(TEST_DATA_PATH, "original", "year=2025", "month=01")
STAGING_LOCATION = os.path.join(TEST_DATA_PATH, "staging", "year=2025", "month=01")


class MockSparkSession:
    """Mock Spark session that reads local parquet files."""
    
    class MockDataFrame:
        def __init__(self, path: str):
            self.path = path
            self._count = None
        
        def count(self) -> int:
            """Count rows by reading local parquet files."""
            import pandas as pd
            
            # Convert s3:// path to local path
            local_path = self._resolve_local_path(self.path)
            
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"Path not found: {local_path}")
            
            total_rows = 0
            if os.path.isdir(local_path):
                for file in os.listdir(local_path):
                    if file.endswith(".parquet"):
                        file_path = os.path.join(local_path, file)
                        df = pd.read_parquet(file_path)
                        total_rows += len(df)
            else:
                df = pd.read_parquet(local_path)
                total_rows = len(df)
            
            return total_rows
        
        def _resolve_local_path(self, path: str) -> str:
            """Convert S3-style path to local test data path."""
            # Remove trailing slash
            path = path.rstrip("/")
            
            # Handle s3:// URIs
            if path.startswith("s3://"):
                # s3://bucket-sor/my_test_table/year=2025/month=01/
                # or s3://bucket-sor/temp_resized_tables/my_test_table/year=2025/month=01/
                # Extract path part after bucket name
                parts = path.replace("s3://", "").split("/", 1)
                if len(parts) > 1:
                    path = parts[1]
            
            # Map to local test directories
            # Check if it's a staging path (contains temp_resized_tables)
            if "temp_resized_tables" in path:
                return STAGING_LOCATION
            else:
                return ORIGINAL_LOCATION
    
    class MockRead:
        def __init__(self, spark):
            self.spark = spark
        
        def parquet(self, path: str):
            return MockSparkSession.MockDataFrame(path)
    
    def __init__(self):
        self.read = MockSparkSession.MockRead(self)


# Create mock clients
mock_glue_client = create_mock_glue_client(
    database=DATABASE_NAME,
    table=TABLE_NAME,
    partition_keys=["year", "month"],
    original_partitions=[
        {
            "values": ["2025", "01"],
            "location": f"s3://{BUCKET_NAME}/temp_resized_tables/{TABLE_NAME}/year=2025/month=01/",
        },
    ],
    staging_partitions=[],
)

mock_s3_client = create_mock_s3_client(
    objects={
        f"{TABLE_NAME}/year=2025/month=01": [
            f"{TABLE_NAME}/year=2025/month=01/part-00000.parquet",
            f"{TABLE_NAME}/year=2025/month=01/part-00001.parquet",
            f"{TABLE_NAME}/year=2025/month=01/part-00002.parquet",
        ],
    },
    hours_ago_modified=5,  # Files modified 5 hours ago (passes 3-hour threshold)
)

mock_spark = MockSparkSession()


# --- Refactored comparison job functions for testing ---

def get_table_partitions(glue_client, database: str, table: str) -> list:
    """Retrieve all partitions for a given Glue Catalog table."""
    partitions = []
    paginator = glue_client.get_paginator("get_partitions")
    for page in paginator.paginate(DatabaseName=database, TableName=table):
        partitions.extend(page["Partitions"])
    return partitions


def get_partition_keys(glue_client, database: str, table: str) -> list:
    """Get partition key names from the table definition."""
    response = glue_client.get_table(DatabaseName=database, Name=table)
    return [key["Name"] for key in response["Table"]["PartitionKeys"]]


def count_rows_at_location(spark, location: str) -> int:
    """Read parquet at a given location and return the row count."""
    try:
        df = spark.read.parquet(location)
        return df.count()
    except Exception as e:
        print(f"   Error reading {location}: {e}")
        return -1


def get_latest_modified_time(s3_client, bucket: str, prefix: str):
    """Get the most recent LastModified timestamp across all objects."""
    latest = None
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            modified = obj["LastModified"]
            if latest is None or modified > latest:
                latest = modified

    return latest


def run_comparison_job(
    glue_client,
    s3_client,
    spark,
    database_name: str,
    table_name: str,
    bucket_name: str,
    original_prefix: str,
    hours_threshold: int,
) -> bool:
    """
    Main comparison logic extracted for testing.
    Returns True if comparison passed, False otherwise.
    """
    import json
    from datetime import datetime, timezone, timedelta
    
    comparison_passed = True
    now = datetime.now(timezone.utc)
    threshold_time = now - timedelta(hours=hours_threshold)

    partition_keys = get_partition_keys(glue_client, database_name, table_name)
    partitions = get_table_partitions(glue_client, database_name, table_name)

    if not partitions:
        print(f"No partitions found for {database_name}.{table_name}.")
        comparison_passed = False
        return comparison_passed

    print(
        f"Comparing {len(partitions)} partitions for "
        f"{database_name}.{table_name}"
    )

    for partition in partitions:
        partition_values = partition["Values"]
        partition_dict = dict(zip(partition_keys, partition_values))
        current_location = partition["StorageDescriptor"]["Location"]

        # Build the original location path for file_modified_time check
        partition_path = "/".join(
            f"{key}={value}" for key, value in partition_dict.items()
        )
        original_location = f"s3://{bucket_name}/{original_prefix}/{partition_path}"
        original_s3_prefix = f"{original_prefix}/{partition_path}"

        print(f" Partition {partition_dict}:")

        # -- Check 1: Row count comparison
        staging_count = count_rows_at_location(spark, current_location)
        original_count = count_rows_at_location(spark, original_location)

        print(
            f"   Row count - Original: {original_count}, "
            f"Staging: {staging_count}"
        )

        if original_count < 0 or staging_count < 0:
            print("   FAIL: Could not read one or both locations.")
            comparison_passed = False
            break

        if original_count != staging_count:
            print(
                f"   FAIL: Row count mismatch "
                f"(original={original_count}, staging={staging_count})"
            )
            comparison_passed = False
            break

        # -- Check 2: File modification time comparison
        latest_modified = get_latest_modified_time(
            s3_client, bucket_name, original_s3_prefix
        )

        if latest_modified is not None:
            print(f"   Latest file modification: {latest_modified.isoformat()}")
            if latest_modified > threshold_time:
                print(
                    f"   FAIL: Files were modified after threshold "
                    f"({threshold_time.isoformat()}). "
                    f"Another process may have written to the original path."
                )
                comparison_passed = False
                break
            else:
                print(
                    f"   PASS: No modifications since {threshold_time.isoformat()}"
                )
        else:
            print(
                f"   WARNING: No objects found at original prefix "
                f"{original_s3_prefix}. Treating as passed."
            )

        print(f"   PASS: Partition {partition_dict} validated.")

    # -- Output result -------------------------------
    result = {"comparisonPassed": comparison_passed}
    print(f"\nComparison result: {json.dumps(result)}")

    if comparison_passed:
        print("ALL PARTITIONS VALIDATED SUCCESSFULLY.")
    else:
        print("COMPARISON FAILED - Step Function will retry from Step 1.")

    return comparison_passed


def main():
    """Main entry point for local testing."""
    print("=" * 60)
    print("Running small_files_comparison_job locally with mocks")
    print("=" * 60)
    print(f"Database: {DATABASE_NAME}")
    print(f"Table: {TABLE_NAME}")
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Original Prefix: {ORIGINAL_PREFIX}")
    print(f"Hours threshold: {HOURS_THRESHOLD}")
    print("=" * 60)
    print()
    
    # Run the comparison job with mocked clients
    result = run_comparison_job(
        glue_client=mock_glue_client,
        s3_client=mock_s3_client,
        spark=mock_spark,
        database_name=DATABASE_NAME,
        table_name=TABLE_NAME,
        bucket_name=BUCKET_NAME,
        original_prefix=ORIGINAL_PREFIX,
        hours_threshold=HOURS_THRESHOLD,
    )
    
    print()
    print("=" * 60)
    print(f"Final result: {'PASSED ✓' if result else 'FAILED ✗'}")
    print("=" * 60)
    
    return result


if __name__ == "__main__":
    # Set a breakpoint here to start debugging
    main()
