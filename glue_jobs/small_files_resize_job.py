import sys
import json
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext

print("IMPORTS")

# --- Glue / Spark initialization ---
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "CONFIG_PAYLOAD",
        "TARGET_FILE_SIZE_MB",
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
TARGET_FILE_SIZE_MB = int(
    args.get("TARGET_FILE_SIZE_MB") or config_payload.get("target_file_size_mb", "128")
)

glue_client = boto3.client("glue")
s3_client = boto3.client("s3")

print("DEFINE CLIENTS")
print(f"DATABASE: {DATABASE_NAME}")
print(f"TABLE: {TABLE_NAME}")
print(f"TARGET FILE SIZE (MB): {TARGET_FILE_SIZE_MB}")


# --- Helper functions ---


def get_table_info(database: str, table: str) -> dict:
    """Get table metadata including format and SerDe info from Glue Catalog."""
    response = glue_client.get_table(DatabaseName=database, Name=table)
    return response["Table"]


def get_partition_keys(table_info: dict) -> list:
    """Extract partition key names from table metadata."""
    return [key["Name"] for key in table_info.get("PartitionKeys", [])]


def get_output_format(table_info: dict) -> str:
    """
    Infer the Spark write format from the Glue table StorageDescriptor.
    Defaults to parquet.
    """
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
    # Fallback — Delta tables use a different path entirely; not handled here.
    return "parquet"


def calculate_num_output_files(partition_size_mb: float, target_file_size_mb: int) -> int:
    """
    Calculate how many output files we should coalesce to based on
    the partition size and the target file size.
    """
    if partition_size_mb <= 0:
        return 1
    num_files = max(1, round(partition_size_mb / target_file_size_mb))
    return num_files


def update_partition_location(
    database: str,
    table: str,
    partition_values: list,
    new_location: str,
    table_info: dict,
) -> None:
    """
    Update a Glue Catalog partition's StorageDescriptor Location
    to point to the staging path.
    """
    # Copy existing StorageDescriptor and patch the location
    storage_descriptor = dict(table_info["StorageDescriptor"])
    storage_descriptor["Location"] = new_location

    # Remove read-only keys that Glue rejects on batch_update_partition
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


# --- Main logic ---


def main():
    print("Starting resize job...")

    table_info = get_table_info(DATABASE_NAME, TABLE_NAME)
    partition_keys = get_partition_keys(table_info)
    output_format = get_output_format(table_info)

    print(f"  Partition keys : {partition_keys}")
    print(f"  Output format  : {output_format}")

    partitions = config_payload.get("partitions", [])

    if not partitions:
        print("No partitions to process. Exiting.")
        job.commit()
        return

    print(f"  Partitions to resize: {len(partitions)}")

    failed_partitions = []

    for part_info in partitions:
        partition_values = part_info["partition_values"]
        partition_key_str = part_info["staging_partition_key_str"]
        original_s3_path = part_info["s3_path"]
        staging_s3_path = part_info["staging_s3_path"]
        partition_size_mb = part_info.get("partition_size_mb", 0)
        number_of_objects = part_info.get("number_of_objects", 0)

        print(f"\n  Processing partition: {partition_key_str}")
        print(f"    Original path  : {original_s3_path}")
        print(f"    Staging path   : {staging_s3_path}")
        print(f"    Current files  : {number_of_objects}")
        print(f"    Partition size : {partition_size_mb} MB")

        try:
            # --- Read the partition ---
            df = spark.read.format(output_format).load(original_s3_path)
            original_count = df.count()
            print(f"    Row count      : {original_count}")

            if original_count == 0:
                print("    WARNING: Partition is empty. Skipping.")
                continue

            # --- Calculate target number of output files ---
            num_output_files = calculate_num_output_files(
                partition_size_mb, TARGET_FILE_SIZE_MB
            )
            print(f"    Target files   : {num_output_files}")

            # --- Coalesce and write to staging ---
            # coalesce avoids a full shuffle; use repartition if data is very skewed.
            resized_df = df.coalesce(num_output_files)

            (
                resized_df.write.format(output_format)
                .mode("overwrite")
                .save(staging_s3_path)
            )

            # --- Validate staging write ---
            staging_df = spark.read.format(output_format).load(staging_s3_path)
            staging_count = staging_df.count()
            print(f"    Staging count  : {staging_count}")

            if staging_count != original_count:
                raise ValueError(
                    f"Row count mismatch after write: "
                    f"original={original_count}, staging={staging_count}"
                )

            # --- Update Glue Catalog partition pointer to staging ---
            update_partition_location(
                DATABASE_NAME,
                TABLE_NAME,
                partition_values,
                staging_s3_path,
                table_info,
            )

            print(f"    PASS: Partition {partition_key_str} resized and catalog updated.")

        except Exception as e:
            print(f"    ERROR processing partition {partition_key_str}: {e}")
            failed_partitions.append(partition_key_str)

    if failed_partitions:
        raise Exception(
            f"Resize job failed for partitions: {failed_partitions}. "
            "Step Function will handle retry."
        )

    print("\nAll partitions resized successfully.")
    job.commit()


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print("ERROR:", err)
        raise
