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

# MAGIC %%sql
# MAGIC SELECT * FROM LHDEVBRONZE.control.cfg_bronze_staging;
# MAGIC SELECT * FROM LHDEVBRONZE.control.cfg_bronze_raw;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC update LHDEVBRONZE.control.cfg_bronze_staging
# MAGIC set ISACTIVE = 'Y'
# MAGIC where BRONZE_STAGING_ENTITY_ID = 1
# MAGIC ;
# MAGIC update LHDEVBRONZE.control.cfg_bronze_staging
# MAGIC set ISACTIVE = 'N'
# MAGIC where BRONZE_STAGING_ENTITY_ID = 2
# MAGIC ;
# MAGIC update LHDEVBRONZE.control.cfg_bronze_raw
# MAGIC set ISACTIVE = 'Y'
# MAGIC where BRONZE_RAW_ENTITY_ID = 1
# MAGIC ;
# MAGIC update LHDEVBRONZE.control.cfg_bronze_raw
# MAGIC set ISACTIVE = 'N'
# MAGIC where BRONZE_RAW_ENTITY_ID = 2
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
