# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "14c8601e-2d9e-4a4a-9db0-b0227766c429",
# META       "default_lakehouse_name": "LHDEVBRONZE",
# META       "default_lakehouse_workspace_id": "d021c3f1-8374-419e-87ab-d3d45db22fb1",
# META       "known_lakehouses": [
# META         {
# META           "id": "14c8601e-2d9e-4a4a-9db0-b0227766c429"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import current_timestamp
from datetime import datetime

safe_error_message = error_message[:3500] if error_message else None

schema = StructType([
    StructField("BATCH_ID", StringType(), True),
    StructField("PIPELINE_NAME", StringType(), True),
    StructField("PIPELINE_ID", StringType(), True),
    StructField("PIPELINE_RUN_ID", StringType(), True),
    StructField("ACTIVITY_NAME", StringType(), True),
    StructField("ERROR_MESSAGE", StringType(), True),
    StructField("ACTIVITY_TYPE", StringType(), True),
    StructField("ACTIVITY_START_TIME", TimestampType(), True),
    StructField("ACTIVITY_END_TIME", TimestampType(), True),
])

df = spark.createDataFrame([(
    batch_id,
    pipeline_name,
    pipeline_id,
    pipeline_run_id,
    activity_name,
    safe_error_message,
    activity_type,
    datetime.fromisoformat(activity_start_time.replace("Z","")),
    datetime.fromisoformat(activity_end_time.replace("Z",""))
)], schema=schema).withColumn("CREATED_DATE", current_timestamp())

df.write.mode("append").saveAsTable("audit.ops_error_log")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
