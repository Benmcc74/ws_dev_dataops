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

from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime

try:
    
    schema = StructType([
        StructField("BATCH_ID", StringType(), True),
        StructField("LAYER", StringType(), True),
        StructField("PIPELINE_ID", StringType(), True),
        StructField("PIPELINE_RUN_ID", StringType(), True),
        StructField("PIPELINE_NAME", StringType(), True),
        StructField("START_TIME", TimestampType(), True),
        StructField("END_TIME", TimestampType(), True),
        StructField("WORKSPACE_ID", StringType(), True),
        StructField("PARENT_PIPELINE_ID", StringType(), True),
        StructField("PARENT_PIPELINE_RUN_ID", StringType(), True),
        StructField("PARENT_PIPELINE_NAME", StringType(), True),
        StructField("PIPELINE_STATUS", StringType(), True)
    ])

    row = Row(
        BATCH_ID=batch_id,
        LAYER=layer,
        PIPELINE_ID=pipeline_id,
        PIPELINE_RUN_ID=pipeline_run_id,
        PIPELINE_NAME=pipeline_name,
        START_TIME=datetime.utcnow(),
        END_TIME=None,
        WORKSPACE_ID=workspace_id,
        PARENT_PIPELINE_ID=parent_pipeline_id,
        PARENT_PIPELINE_RUN_ID=parent_pipeline_run_id,
        PARENT_PIPELINE_NAME=parent_pipeline_name,
        PIPELINE_STATUS=pipeline_status
    )

    df = spark.createDataFrame([row], schema=schema)

    df.write.mode("append").saveAsTable("audit.ops_pipeline_runs")

    print("Inserted audit record for:", pipeline_run_id)
    

except Exception as e:
   raise Exception(f"invalid: {str(e)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
