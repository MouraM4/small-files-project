aws glue update-partition \
    --database-name sot_database \
    --table-name my_test_tablemy_test_table \
    --partition-value-list "2025" "01" \
    --partition-input '{
        "Values": ["2025", "01"],
        "StorageDescriptor": {
            "Location": "s3://sot-layer/temp_resized_tables/my_test_table/year=2025/month=01/",
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            }
        }
    }'
