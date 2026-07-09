# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Silver Layer Master Pipeline - Shared Configuration
# MAGIC %md
# MAGIC # Silver Layer Master Pipeline
# MAGIC
# MAGIC This master notebook orchestrates all silver layer transformations with a shared batch_id.
# MAGIC
# MAGIC **Included Notebooks:**
# MAGIC - `silver_crm_customers` - Customer data transformations
# MAGIC - `silver_crm_products` - Product data transformations
# MAGIC
# MAGIC **Execution:** Running this notebook will execute all child notebooks in sequence with the same batch_id.

# COMMAND ----------

# DBTITLE 1,Setup Shared Configuration
import uuid
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp

# Shared configuration for all silver layer transformations
SILVER_DB = "sqldatawarehouse.silver"
AUDIT_TABLE = f"{SILVER_DB}.transformation_audit"

# Generate a single batch_id for this entire pipeline run
batch_id = str(uuid.uuid4())

print(f"Starting Silver Layer Pipeline")
print(f"Batch ID: {batch_id}")
print(f"Audit Table: {AUDIT_TABLE}")

# COMMAND ----------

# DBTITLE 1,Define Shared Audit Function
def log_silver_audit(source_table, target_table, row_count, transformations_applied, status):
    """
    Log silver layer transformation to audit table
    
    Args:
        source_table: Bronze table(s) used as source
        target_table: Silver table written to
        row_count: Number of rows in the result
        transformations_applied: Description of transformations
        status: SUCCESS or FAILED
    """
    audit_df = spark.createDataFrame([(
        source_table,
        target_table,
        row_count,
        batch_id,  # Uses the batch_id from parent scope
        transformations_applied,
        status,
        datetime.now()
    )], [
        "source_table",
        "target_table",
        "row_count",
        "batch_id",
        "transformations_applied",
        "status",
        "transformation_time"
    ])
    
    audit_df.write.mode("append").saveAsTable(AUDIT_TABLE)

print("✓ Audit function defined")

# COMMAND ----------

# DBTITLE 1,Execute Products Transformations
print("\n" + "="*60)
print("STEP 1: Processing CRM Products")
print("="*60)

dbutils.notebook.run("/Repos/lkhant616@gmail.com/Data_Port/Data_Engineering/Silver/silver_crm_products", 0, {"batch_id": batch_id})

print("\n✓ Products processing complete")

# COMMAND ----------

# DBTITLE 1,Execute Customer Transformations
print("\n" + "="*60)
print("STEP 2: Processing CRM Customers")
print("="*60)

dbutils.notebook.run("/Repos/lkhant616@gmail.com/Data_Port/Data_Engineering/Silver/silver_crm_customer", 0, {"batch_id": batch_id})

print("\n✓ Customers processing complete")

# COMMAND ----------

# DBTITLE 1,Pipeline Summary
print("\n" + "="*60)
print("PIPELINE COMPLETE")
print("="*60)

print(f"\nBatch ID: {batch_id}")

# Show audit log for this batch
print("\nAudit Summary:")
spark.sql(f"""
    SELECT 
        target_table,
        row_count,
        status,
        transformation_time
    FROM {AUDIT_TABLE}
    WHERE batch_id = '{batch_id}'
    ORDER BY transformation_time
""").show(truncate=False)

print("\n✓ All silver layer transformations complete with shared batch_id!")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from sqldatawarehouse.silver.crm_customers
