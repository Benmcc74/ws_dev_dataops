# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a2c3ae2e-6fd2-41b6-bde4-1529a8103742",
# META       "default_lakehouse_name": "LHDEVGOLDSTAGING",
# META       "default_lakehouse_workspace_id": "7b94f468-80f9-400a-a564-dfaacf1dab5f",
# META       "known_lakehouses": [
# META         {
# META           "id": "a2c3ae2e-6fd2-41b6-bde4-1529a8103742"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession


# create Spark session
spark = SparkSession.builder \
    .appName("Refresh Lakehouse SQL Endpoint") \
    .getOrCreate()
 
# define schema name
schema_name = "silver"
 
# get all tables in schema
tables_df = spark.sql(f"SHOW TABLES IN {schema_name}")
tables = [row.tableName for row in tables_df.collect()]
 
# refresh each table
for table in tables:  
    spark.sql(f"REFRESH TABLE {schema_name}.{table}")
    print(f"Completed : Refreshing table {schema_name}.{table} ")
 
print(f"Metadata refresh completed for schema: {schema_name}")





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Auto-refresh the SQL Analytics Endpoint (SAE) metadata cache. 
#When the SAE goes idle (~15 min of inactivity), its metadata goes stale.

import sempy.fabric as fabric
import json, time
 

# Get workspace and lakehouse IDs
workspace_id = spark.conf.get("trident.workspace.id")
lakehouse_id = spark.conf.get("trident.lakehouse.id")
 

# Get SQL Analytics Endpoint ID
client = fabric.FabricRestClient()
lakehouse_info = client.get(
    f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
).json()
sql_endpoint_id = lakehouse_info['properties']['sqlEndpointProperties']['id']
 

# Trigger metadata refresh
uri = f"/v1.0/myorg/lhdatamarts/{sql_endpoint_id}"
payload = {"commands": [{"$type": "MetadataRefreshExternalCommand"}]}
response = client.post(uri, json=payload)
 

data = response.json()
batch_id = data["batchId"]
 

# Poll until complete
status_uri = f"/v1.0/myorg/lhdatamarts/{sql_endpoint_id}/batches/{batch_id}"
progress = data["progressState"]
while progress == "inProgress":
    time.sleep(2)
    progress = client.get(status_uri).json()["progressState"]
 

print(f"SAE Refresh completed with status: {progress}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
