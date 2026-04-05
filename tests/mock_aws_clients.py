"""
Mock AWS clients for local testing of the small_files_comparison_job.
"""

from datetime import datetime, timezone, timedelta
from typing import Iterator


class MockGluePaginator:
    """Mock paginator for Glue get_partitions."""
    
    def __init__(self, partitions: list):
        self.partitions = partitions
    
    def paginate(self, DatabaseName: str, TableName: str) -> Iterator[dict]:
        yield {"Partitions": self.partitions}


class MockS3Paginator:
    """Mock paginator for S3 list_objects_v2."""
    
    def __init__(self, objects: dict):
        # objects: dict mapping prefix -> list of object metadata
        self.objects = objects
    
    def paginate(self, Bucket: str, Prefix: str) -> Iterator[dict]:
        contents = self.objects.get(Prefix, [])
        yield {"Contents": contents}


class MockGlueClient:
    """
    Mock Glue client that simulates responses for:
    - get_table
    - get_partitions (via paginator)
    """
    
    def __init__(self, table_config: dict, partitions: list):
        """
        Args:
            table_config: Dict with table metadata including PartitionKeys
            partitions: List of partition dicts with Values and StorageDescriptor
        """
        self.table_config = table_config
        self.partitions = partitions
    
    def get_table(self, DatabaseName: str, Name: str) -> dict:
        return {
            "Table": {
                "Name": Name,
                "DatabaseName": DatabaseName,
                "PartitionKeys": self.table_config.get("PartitionKeys", []),
                "StorageDescriptor": self.table_config.get("StorageDescriptor", {"Location": f"s3://default-bucket/{Name}/"}),
            }
        }
    
    def get_paginator(self, operation_name: str):
        if operation_name == "get_partitions":
            return MockGluePaginator(self.partitions)
        raise ValueError(f"Unknown paginator: {operation_name}")


class MockS3Client:
    """
    Mock S3 client that simulates responses for:
    - list_objects_v2 (via paginator)
    """
    
    def __init__(self, objects: dict, hours_ago_modified: int = 5):
        """
        Args:
            objects: Dict mapping S3 prefix -> list of object keys
            hours_ago_modified: How many hours ago the files were last modified
        """
        self.hours_ago_modified = hours_ago_modified
        # Convert simple key list to full object metadata
        self.objects = {}
        modified_time = datetime.now(timezone.utc) - timedelta(hours=hours_ago_modified)
        
        for prefix, keys in objects.items():
            self.objects[prefix] = [
                {
                    "Key": key,
                    "LastModified": modified_time,
                    "Size": 1024,
                }
                for key in keys
            ]

    def put_object(self, **kwargs):
        # Mock put_object mapping S3 prefix -> data
        bucket = kwargs.get("Bucket")
        key = kwargs.get("Key")
        body = kwargs.get("Body")
        if not hasattr(self, "put_objects"):
            self.put_objects = {}
        self.put_objects[f"s3://{bucket}/{key}"] = body
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}
    
    def get_paginator(self, operation_name: str):
        if operation_name == "list_objects_v2":
            return MockS3Paginator(self.objects)
        raise ValueError(f"Unknown paginator: {operation_name}")
        
    def delete_objects(self, **kwargs):
        # Mock delete_objects response
        bucket = kwargs.get("Bucket")
        delete = kwargs.get("Delete", {})
        objects_to_delete = delete.get("Objects", [])
        
        deleted = []
        for obj in objects_to_delete:
            key = obj.get("Key")
            # Find and remove from mock dictionary
            for prefix, keys in self.objects.items():
                self.objects[prefix] = [k for k in keys if k["Key"] != key]
            deleted.append({"Key": key})
            
        return {"Deleted": deleted}


def create_mock_glue_client(
    database: str,
    table: str,
    partition_keys: list[str],
    original_partitions: list[dict],
    staging_partitions: list[dict],
) -> MockGlueClient:
    """
    Factory function to create a configured MockGlueClient.
    
    Args:
        database: Database name
        table: Table name
        partition_keys: List of partition key names, e.g., ["year", "month"]
        original_partitions: List of dicts with 'values' and 'location'
        staging_partitions: List of dicts with 'values' and 'location'
    
    Example:
        client = create_mock_glue_client(
            database="my_db",
            table="my_table",
            partition_keys=["year", "month"],
            original_partitions=[
                {"values": ["2025", "01"], "location": "s3://bucket/data/year=2025/month=01/"}
            ],
            staging_partitions=[
                {"values": ["2025", "01_staging"], "location": "s3://bucket/staging/year=2025/month=01_staging/"}
            ],
        )
    """
    table_config = {
        "PartitionKeys": [{"Name": key} for key in partition_keys],
    }
    
    partitions = []
    
    for p in original_partitions:
        partitions.append({
            "Values": p["values"],
            "StorageDescriptor": {"Location": p["location"]},
        })
    
    for p in staging_partitions:
        partitions.append({
            "Values": p["values"],
            "StorageDescriptor": {"Location": p["location"]},
        })
    
    return MockGlueClient(table_config, partitions)


def create_mock_s3_client(
    objects: dict,
    hours_ago_modified: int = 5,
) -> MockS3Client:
    """
    Factory function to create a configured MockS3Client.
    
    Args:
        objects: Dict mapping S3 prefix -> list of object keys
        hours_ago_modified: How many hours ago files were modified (default: 5)
    
    Example:
        client = create_mock_s3_client(
            objects={
                "data/year=2025/month=01/": ["file1.parquet", "file2.parquet"],
            },
            hours_ago_modified=5,
        )
    """
    return MockS3Client(objects, hours_ago_modified)
