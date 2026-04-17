import sys
import json
from datetime import datetime, timezone, timedelta

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

print("IMPORTS")

# --- Glue / Spark initialization ---
args = getResolvedOptions(
    sys.argv,
    [
        "CONFIG_PAYLOAD",
        "HOURS_THRESHOLD",
    ],
)

print("ARGS")

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)

print("DEFINE SPARK CONTEXT")

config_payload = json.loads(args["CONFIG_PAYLOAD"])
DATABASE_NAME = config_payload["params"]["database_name"]
TABLE_NAME = config_payload["params"]["table_name"]
BUCKET_NAME = config_payload["params"]["bucket_name"]
HOURS_THRESHOLD = int(args.get("hours_threshold", "3"))

s3_client = boto3.client("s3")
glue_client = boto3.client("glue")

print("DEFINE CLIENTS")

# --- Helper functions ---

def get_table_partitions(database: str, table: str) -> list:
    """Retrieve all partitions for a given Glue Catalog table."""
    partitions = []
    paginator = glue_client.get_paginator("get_partitions")
    for page in paginator.paginate(DatabaseName=database, TableName=table):
        partitions.extend(page["Partitions"])
    return partitions

def get_partition_keys(database: str, table: str) -> list:
    """Get partition key names from the table definition."""
    response = glue_client.get_table(DatabaseName=database, Name=table)
    return [key["Name"] for key in response["Table"]["PartitionKeys"]]

def count_rows_at_location(location: str) -> int:
    """Read parquet at a given S3 location and return the row count."""
    try:
        df = spark.read.parquet(location)
        return df.count()
    except Exception as e:
        print(f" Error reading {location}: {e}")
        return -1

def get_latest_modified_time(bucket: str, prefix: str) -> datetime | None:
    """
    Get the most recent LastModified timestamp across all objects
    under the given S3 prefix.
    """
    latest = None
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            modified = obj["LastModified"]
            if latest is None or modified > latest:
                latest = modified
    return latest

def extract_prefix_from_location(location: str) -> str:
    """Extract the S3 prefix (key) from a full s3:// URI."""
    # s3://bucket/path/to/data/ -> path/to/data/
    parts = location.replace("s3://", "", 1).split("/", 1)
    return parts[1] if len(parts) > 1 else ""

# --- Main logic ---

def main():
    comparison_passed = True
    now = datetime.now(timezone.utc)
    threshold_time = now - timedelta(hours=HOURS_THRESHOLD)

    print("DATABASE NAME: ", DATABASE_NAME)
    print("TABLE NAME: ", TABLE_NAME)

    partitions = config_payload.get("partitions", [])

    if not partitions:
        print(f"No partitions defined dynamically for {DATABASE_NAME}.{TABLE_NAME}.")
        comparison_passed = False
        return comparison_passed

    print(
        f"Comparing {len(partitions)} partitions for "
        f"{DATABASE_NAME}.{TABLE_NAME}"
    )

    for part_info in partitions:
        partition_values = part_info["partition_values"]
        partition_key_str = part_info["staging_partition_key_str"]
        staging_s3_path = part_info["staging_s3_path"]
        original_location = part_info["s3_path"]
        original_s3_prefix = part_info["s3_path_prefix"]
        
        print(f" Partition {partition_key_str}:")

        # -- Check 1: Row count comparison
        # The staging location is where the catalog currently points after swap, but we can verify explicitly
        staging_count = count_rows_at_location(staging_s3_path)

        # Read from the original location to compare
        original_count = count_rows_at_location(original_location)

        print(
            f" Row count - Original: {original_count}, "
            f"Staging: {staging_count}"
        )

        if original_count < 0 or staging_count < 0:
            print(" FAIL: Could not read one or both locations.")
            comparison_passed = False
            break

        if original_count != staging_count:
            print(
                f" FAIL: Row count mismatch "
                f"(original={original_count}, staging={staging_count})"
            )
            comparison_passed = False
            break

        # -- Check 2: File modification time comparison
        latest_modified = get_latest_modified_time(
            BUCKET_NAME, original_s3_prefix
        )

        if latest_modified is not None:
            print(f"  Latest file modification: {latest_modified.isoformat()}")
            if latest_modified > threshold_time:
                print(
                    f"  FAIL: Files were modified after threshold "
                    f"({threshold_time.isoformat()}). "
                    f"Another process may have written to the original path."
                )
                comparison_passed = False
                break
            else:
                print(
                    f"  PASS: No modifications since {threshold_time.isoformat()}"
                )
        else:
            print(
                f"  WARNING: No objects found at original prefix "
                f"{original_s3_prefix}. Treating as passed."
            )

        print(f"  PASS: Partition {partition_key_str} validated.")

    # -- Output result for Step Function -------------------------------
    result = {"comparisonPassed": comparison_passed}
    print(f"\nComparison result: {json.dumps(result)}")

    # Write result to a known S3 location for the Step Function to pick up
    # The Glue job output arguments are passed back via the job bookmark / SF integration
    if comparison_passed:
        print("ALL PARTITIONS VALIDATED SUCCESSFULLY.")
    else:
        raise Exception("COMPARISON FAILED – Step Function will retry from Step 1.")

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print("ERROR:", err)
        raise err