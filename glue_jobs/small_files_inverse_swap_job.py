import sys
import json

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
        "JOB_NAME",
        "CONFIG_PAYLOAD",
    ],
)

print("ARGS")

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

print("DEFINE SPARK CONTEXT")

config_payload = json.loads(args["CONFIG_PAYLOAD"])
DATABASE_NAME = config_payload["database_name"]
TABLE_NAME = config_payload["table_name"]
BUCKET_NAME = config_payload["bucket_name"]

s3_client = boto3.client("s3")
glue_client = boto3.client("glue")

print("DEFINE CLIENTS")
print(f"DATABASE: {DATABASE_NAME}")
print(f"TABLE: {TABLE_NAME}")
print(f"BUCKET: {BUCKET_NAME}")


# --- Helper functions ---


def get_table_info(database: str, table: str) -> dict:
    """Get table metadata from Glue Catalog."""
    response = glue_client.get_table(DatabaseName=database, Name=table)
    return response["Table"]


def list_objects(bucket: str, prefix: str) -> list:
    """
    List all objects under a given S3 prefix.
    Returns a list of object keys (strings).
    """
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def copy_object(bucket: str, source_key: str, dest_key: str) -> None:
    """Copy a single S3 object within the same bucket."""
    s3_client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key,
    )


def delete_objects(bucket: str, keys: list) -> None:
    """
    Delete a list of S3 object keys in batches of 1000
    (the S3 deleteObjects API limit).
    """
    batch_size = 1000
    for i in range(0, len(keys), batch_size):
        batch = keys[i : i + batch_size]
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch]},
        )
        print(f"    Deleted {len(batch)} objects.")


def extract_prefix_from_uri(s3_uri: str) -> str:
    """Extract the key prefix from an s3:// URI."""
    # s3://bucket/prefix/path/ -> prefix/path/
    parts = s3_uri.replace("s3://", "", 1).split("/", 1)
    return parts[1] if len(parts) > 1 else ""


def copy_staging_to_original(
    bucket: str,
    staging_prefix: str,
    original_prefix: str,
) -> int:
    """
    Copy all objects from the staging prefix to the original prefix.
    Preserves relative key structure.
    Returns the number of objects copied.
    """
    staging_keys = list_objects(bucket, staging_prefix)

    if not staging_keys:
        print(f"    WARNING: No objects found at staging prefix {staging_prefix}.")
        return 0

    print(f"    Copying {len(staging_keys)} objects from staging → original.")

    for source_key in staging_keys:
        # Compute the relative path inside the staging prefix
        relative_path = source_key[len(staging_prefix):]
        dest_key = original_prefix.rstrip("/") + "/" + relative_path.lstrip("/")
        copy_object(bucket, source_key, dest_key)

    print(f"    Copy complete: {len(staging_keys)} objects.")
    return len(staging_keys)


def update_partition_location(
    database: str,
    table: str,
    partition_values: list,
    new_location: str,
    table_info: dict,
) -> None:
    """
    Update a Glue Catalog partition's StorageDescriptor Location
    to point back to the original path.
    """
    storage_descriptor = dict(table_info["StorageDescriptor"])
    storage_descriptor["Location"] = new_location

    # Remove read-only keys Glue rejects on update
    for key in ("Parameters",):
        storage_descriptor.pop(key, None)

    glue_client.update_partition(
        DatabaseName=database,
        TableName=table,
        PartitionValueList=partition_values,
        PartitionInput={
            "Values": partition_values,
            "StorageDescriptor": storage_descriptor,
        },
    )
    print(f"    Catalog updated → {new_location}")


def validate_copy(
    bucket: str,
    staging_prefix: str,
    original_prefix: str,
    output_format: str,
) -> bool:
    """
    Validate the copy by comparing row counts between staging and original paths
    using Spark. Returns True if counts match.
    """
    try:
        staging_uri = f"s3://{bucket}/{staging_prefix}"
        original_uri = f"s3://{bucket}/{original_prefix}"

        staging_count = spark.read.format(output_format).load(staging_uri).count()
        original_count = spark.read.format(output_format).load(original_uri).count()

        print(f"    Validation — staging: {staging_count}, original: {original_count}")
        return staging_count == original_count
    except Exception as e:
        print(f"    WARNING: Validation read failed: {e}")
        return False


def get_output_format(table_info: dict) -> str:
    """Infer Spark format from Glue table StorageDescriptor."""
    input_format = table_info["StorageDescriptor"].get("InputFormat", "")
    serde = (
        table_info["StorageDescriptor"]
        .get("SerdeInfo", {})
        .get("SerializationLibrary", "")
    )
    if "parquet" in input_format.lower() or "parquet" in serde.lower():
        return "parquet"
    if "orc" in input_format.lower() or "orc" in serde.lower():
        return "orc"
    if "avro" in input_format.lower() or "avro" in serde.lower():
        return "avro"
    return "parquet"


# --- Main logic ---


def main():
    print("Starting inverse swap job...")

    table_info = get_table_info(DATABASE_NAME, TABLE_NAME)
    output_format = get_output_format(table_info)

    print(f"  Output format: {output_format}")

    partitions = config_payload.get("partitions", [])

    if not partitions:
        print("No partitions to process. Exiting.")
        job.commit()
        return

    print(f"  Partitions to inverse-swap: {len(partitions)}")

    failed_partitions = []

    for part_info in partitions:
        partition_values = part_info["partition_values"]
        partition_key_str = part_info["staging_partition_key_str"]

        # Staging is where the catalog currently points (set by resize job)
        staging_s3_path = part_info["staging_s3_path"]
        staging_prefix = part_info["staging_s3_path_prefix"]

        # Original is the final destination we want to restore to
        original_s3_path = part_info["s3_path"]
        original_prefix = part_info["s3_path_prefix"]

        print(f"\n  Processing partition: {partition_key_str}")
        print(f"    Staging path   : {staging_s3_path}")
        print(f"    Original path  : {original_s3_path}")

        try:
            # Ensure prefix ends with /
            staging_prefix_clean = staging_prefix.rstrip("/") + "/"
            original_prefix_clean = original_prefix.rstrip("/") + "/"

            # Step 1 — Copy compacted files from staging → original path
            num_copied = copy_staging_to_original(
                BUCKET_NAME,
                staging_prefix_clean,
                original_prefix_clean,
            )

            if num_copied == 0:
                print(f"    SKIP: Nothing to copy for partition {partition_key_str}.")
                continue

            # Step 2 — Validate the copy
            is_valid = validate_copy(
                BUCKET_NAME,
                staging_prefix_clean,
                original_prefix_clean,
                output_format,
            )

            if not is_valid:
                raise ValueError(
                    f"Row count mismatch after copy for partition {partition_key_str}. "
                    "Aborting catalog update to preserve data integrity."
                )

            # Step 3 — Update Glue Catalog partition pointer back to original path
            update_partition_location(
                DATABASE_NAME,
                TABLE_NAME,
                partition_values,
                original_s3_path,
                table_info,
            )

            print(f"    PASS: Partition {partition_key_str} restored to original path.")

        except Exception as e:
            print(f"    ERROR on partition {partition_key_str}: {e}")
            failed_partitions.append(partition_key_str)

    if failed_partitions:
        raise Exception(
            f"Inverse swap failed for partitions: {failed_partitions}. "
            "Review and re-run — staging data is still intact."
        )

    print("\nAll partitions inverse-swapped successfully.")
    job.commit()


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print("ERROR:", err)
        raise
