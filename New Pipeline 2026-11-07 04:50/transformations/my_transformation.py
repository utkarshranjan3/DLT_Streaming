import dlt
from pyspark.sql import functions as F

# 1. Bronze Table: Ingests raw customer batches continuously using Auto Loader
# Unity Catalog three-level namespace: catalog.schema.table (e.g., main.default.bronze_customers)
@dlt.table(
    name="main.default.bronze_customers",
    comment="Raw streaming customer data with automatic schema evolution"
)
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "/tmp/dlt_practice/_schema")
        .load("/Volumes/main/default/dlt_streaming/dlt_raw_data/")
        .select(
            F.col("customer_id"),
            F.upper(F.trim(F.col("name"))).alias("name"),
            F.col("city"),
            F.col("tier"),
            F.to_timestamp(F.col("update_date")).alias("update_timestamp"),
            # Captures newly introduced columns (like phone_number) dynamically (Schema Evolution)
            F.col("*") 
        )
    )

# 2. Define the Silver Target Streaming Table for SCD Type 2
dlt.create_streaming_table(
    name="main.default.dim_customer_scd2",
    comment="SCD Type 2 historical customer dimension table managed by DLT"
)

# 3. Apply Changes (SCD Type 2 Engine)
# Automatically handles closing active records, inserting updates, and tracking history
dlt.apply_changes(
    target="main.default.dim_customer_scd2",
    source="bronze_customers",
    keys=["customer_id"],
    sequence_by=F.col("update_timestamp"),
    stored_as_scd_type="2",
    track_history_except_column_list=["update_timestamp"]
)
