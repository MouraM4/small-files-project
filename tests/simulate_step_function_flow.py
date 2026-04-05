"""
Python script to locally step through and simulate the Step Functions ASL logic interactively.
Run in VS Code using the built-in Debugger to pause at line breaks and view S3/Glue mocked behavior.
"""

import sys
import os
import json
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from tests.mock_aws_clients import create_mock_glue_client, create_mock_s3_client
from tests.run_comparison_job_local import main as run_comparison_job_local_main

# -- Mock S3 Setup --
DATABASE_NAME = "my_test_database"
TABLE_NAME = "my_test_table"

# Create a mock S3 client populated with original and staging tables
mock_s3_client = create_mock_s3_client(
    objects={
        # Original partitions
        f"{TABLE_NAME}/year=2025/month=01/": [
            f"{TABLE_NAME}/year=2025/month=01/part-0000{i}.parquet" for i in range(3)
        ],
        f"{TABLE_NAME}/year=2025/month=02/": [
            f"{TABLE_NAME}/year=2025/month=02/part-0000{i}.parquet" for i in range(2)
        ],
        # Staging partitions (simulating the ones created by the Swap job)
        f"temp_resized_tables/{TABLE_NAME}/year=2025/month=01/": [
            f"temp_resized_tables/{TABLE_NAME}/year=2025/month=01/part-00000.parquet"
        ],
        f"temp_resized_tables/{TABLE_NAME}/year=2025/month=02/": [
            f"temp_resized_tables/{TABLE_NAME}/year=2025/month=02/part-00000.parquet"
        ],
    }
)


def step_function_simulator():
    print("=" * 60)
    print("Starting Step Function Simulator...")
    print("=" * 60)

    # 1. GetJobParameters (Reads generated_params.json)
    print("\n[State: GetJobParameters]")
    json_path = os.path.join(
        PROJECT_ROOT, "tests", "test_data", "generated_params.json"
    )
    if not os.path.exists(json_path):
        print(
            f" ERROR: Params JSON not found at {json_path}. Run run_parameter_builder_local.py first."
        )
        return

    with open(json_path, "r", encoding="utf-8") as f:
        params = json.load(f)
    print(f" Loaded parameters for {params['partition_count']} partitions.")

    # 2. CheckHasPartitions (Choice)
    print("\n[State: CheckHasPartitions]")
    if params["partition_count"] <= 0:
        print(" No partitions to process. [State: NoPartitionsToProcess] -> Succeed")
        return
    print(" Partitions > 0. Proceeding to Swap...")

    # Main Orchestrator Loop
    while True:
        # 3. SwapGlueJob
        print("\n[State: SwapGlueJob]")
        print(
            f" Executing glue_job_resize... (passing params with {params['number_of_workers']} workers)"
        )
        # In a real environment, this starts the Glue Job asynchronously.
        time.sleep(1)

        # 4. Wait3HoursFirst
        print("\n[State: Wait3HoursFirst]")
        print(" Waiting 3 hours for readers to clear... (simulated sleep 2s)")
        time.sleep(2)

        # 5. ComparisonGlueJob
        print("\n[State: ComparisonGlueJob]")
        print(" Executing small_files_comparison_job...")
        # Mock Response for comparison (Set to False to simulate failure and loop)
        comparison_succeeded = run_comparison_job_local_main()
        time.sleep(1)

        # 6. ChoiceComparisonResult
        print(
            f"\n[State: ChoiceComparisonResult] -> SUCCEEDED = {comparison_succeeded}"
        )
        if comparison_succeeded:
            print(" Validation matched. Proceeding to Deletion.")
            break
        else:
            print(" Validation failed. Looping back to SwapGlueJob.")
            time.sleep(2)

    # Extract dynamic partition array from the mapping
    partitions_array = list(params["partitions"].values())

    # 7. DeleteOriginalFiles
    print("\n[State: DeleteOriginalFiles (Map State)]")
    for part in partitions_array:
        bucket = params["bucket_name"]
        prefix = part["s3_path_prefix"]

        # Simulating S3 ListObjectsV2
        print(f"  -> Listing: s3://{bucket}/{prefix}")
        paginator = mock_s3_client.get_paginator("list_objects_v2")
        contents = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            contents.extend(page.get("Contents", []))

        if not contents:
            print("     No objects found to delete.")
            continue

        objects_to_delete = [{"Key": obj["Key"]} for obj in contents]
        print(f"  -> Deleting {len(objects_to_delete)} exact objects...")

        # Simulating S3 DeleteObjects Call
        response = mock_s3_client.delete_objects(
            Bucket=bucket, Delete={"Objects": objects_to_delete}
        )
        deleted_keys = [d['Key'] for d in response.get('Deleted', [])]
        print(f"     Deleted from mock S3: {deleted_keys}")
        
        # ACTUALLY delete local mock files in test_data if they exist
        for key in deleted_keys:
            # Map key like 'my_test_table/year=2025/month=01/part-00000.parquet'
            local_suffix = key.replace(f"{TABLE_NAME}/", "")
            local_path = os.path.join(PROJECT_ROOT, "tests", "test_data", "original", local_suffix)
            if os.path.exists(local_path):
                os.remove(local_path)
                print(f"     Deleted local file: {local_path}")

    # 8. InverseSwapGlueJob
    print("\n[State: InverseSwapGlueJob]")
    print(f" Executing glue_job_inverse_swap... (simulated)")
    time.sleep(1)

    # 9. Wait10Seconds
    print("\n[State: Wait10Seconds]")
    print(" Waiting... (simulated sleep 1s)")
    time.sleep(1)

    # 10. DeleteStaging
    print("\n[State: DeleteStaging (Map State)]")
    for part in partitions_array:
        bucket = params["bucket_name"]
        staging_prefix = part["staging_s3_path_prefix"]

        # Simulating S3 ListObjectsV2
        print(f"  -> Listing: s3://{bucket}/{staging_prefix}")
        paginator = mock_s3_client.get_paginator("list_objects_v2")
        contents = []
        for page in paginator.paginate(Bucket=bucket, Prefix=staging_prefix):
            contents.extend(page.get("Contents", []))

        if not contents:
            print("     No objects found in staging.")
            continue

        objects_to_delete = [{"Key": obj["Key"]} for obj in contents]
        print(f"  -> Deleting {len(objects_to_delete)} exact staging objects...")

        # Simulating S3 DeleteObjects Call
        response = mock_s3_client.delete_objects(
            Bucket=bucket, Delete={"Objects": objects_to_delete}
        )
        deleted_keys = [d['Key'] for d in response.get('Deleted', [])]
        print(f"     Deleted from mock S3: {deleted_keys}")
        
        # ACTUALLY delete local mock files in test_data if they exist
        for key in deleted_keys:
            # Map key like 'temp_resized_tables/my_test_table/year=2025/month=01/part-00000.parquet'
            local_suffix = key.replace(f"temp_resized_tables/{TABLE_NAME}/", "")
            local_path = os.path.join(PROJECT_ROOT, "tests", "test_data", "staging", local_suffix)
            if os.path.exists(local_path):
                os.remove(local_path)
                print(f"     Deleted local file: {local_path}")

    print("\n============================================================")
    print("✔ Step Function Simulation Completed Successfully")
    print("============================================================")


if __name__ == "__main__":
    # Place VS Code breakpoints here or inside the states to step through your script
    step_function_simulator()
