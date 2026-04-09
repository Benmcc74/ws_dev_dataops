# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql.functions import lit
from datetime import datetime

# Load the audit table
df = spark.table("audit.ops_pipeline_runs")

# Prepare updated values
end_time_value = datetime.utcnow()

# Apply updates to the matching row
df_updated = (
    df
    .withColumn(
        "END_TIME",
        lit(end_time_value).cast("timestamp")
    )
    .withColumn(
        "PIPELINE_STATUS",
        lit(pipeline_status)
    )
)

# Overwrite only the row that matches the pipeline_run_id
(
    df_updated
    .filter(df_updated.PIPELINE_RUN_ID == pipeline_run_id)
    .write
    .mode("overwrite")
    .option("replaceWhere", f"PIPELINE_RUN_ID = '{pipeline_run_id}'")
    .saveAsTable("audit.ops_pipeline_runs")
)

print(f"Updated audit record for pipeline run: {pipeline_run_id}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
