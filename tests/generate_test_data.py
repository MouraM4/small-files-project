"""
Generate test parquet data for local debugging of small_files_comparison_job.

This script creates parquet files in both original and staging directories
to simulate the scenario where resize_glue_job has successfully processed
a partition.
"""

import os
import pandas as pd

# Base path for test data
try:
    BASE_PATH = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Fallback for interactive environments (like Jupyter/IPython)
    BASE_PATH = os.getcwd()
TEST_DATA_PATH = os.path.join(BASE_PATH, "tests/test_data")

# Paths for original and staging partitions
ORIGINAL_PATH = os.path.join(TEST_DATA_PATH, "original", "year=2025", "month=01")
STAGING_PATH = os.path.join(TEST_DATA_PATH, "staging", "year=2025", "month=01")


def generate_test_data(num_rows: int = 100) -> pd.DataFrame:
    """Generate a simple test DataFrame."""
    df = pd.DataFrame({
        "id": range(1, num_rows + 1),
        "name": [f"record_{i}" for i in range(1, num_rows + 1)],
        "value": [i * 10.5 for i in range(1, num_rows + 1)],
        "year": "2025",
        "month": "01"
    })
    
    # Force types to match Glue catalog schema
    df["id"] = df["id"].astype("int32")
    df["value"] = df["value"].astype("float64")
    df["name"] = df["name"].astype(str)
    df["year"] = df["year"].astype(str)
    df["month"] = df["month"].astype(str)
    
    return df


def create_parquet_files():
    """Create parquet files in original and staging directories."""
    
    # Ensure directories exist
    os.makedirs(ORIGINAL_PATH, exist_ok=True)
    os.makedirs(STAGING_PATH, exist_ok=True)
    
    # Generate the same data for both (simulating successful resize)
    df = generate_test_data(num_rows=100)
    
    # Original partition: multiple small files (simulating small files problem)
    print(f"Creating original partition files at: {ORIGINAL_PATH}")
    chunk_size = 20
    for i, start in enumerate(range(0, len(df), chunk_size)):
        chunk = df.iloc[start:start + chunk_size]
        file_path = os.path.join(ORIGINAL_PATH, f"part-{i:05d}.parquet")
        chunk.to_parquet(file_path, index=False)
        print(f"  Created: {file_path} ({len(chunk)} rows)")
    
    # Staging partition: single consolidated file (after resize)
    print(f"\nCreating staging partition file at: {STAGING_PATH}")
    staging_file = os.path.join(STAGING_PATH, "part-00000.parquet")
    df.to_parquet(staging_file, index=False)
    print(f"  Created: {staging_file} ({len(df)} rows)")
    
    print("\n✓ Test data generated successfully!")
    print(f"  Original partition: {len(os.listdir(ORIGINAL_PATH))} files")
    print(f"  Staging partition: {len(os.listdir(STAGING_PATH))} files")




if __name__ == "__main__":
    create_parquet_files()



