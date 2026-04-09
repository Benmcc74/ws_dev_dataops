# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

tables_config = [
    {
        "source_table": "LHDEVBRONZE.bronze_staging.customer_bronze_snapshot",
        "target_table": "LHDEVSILVER.silver.communication_preferences",
        "business_key": "id",
        "business_key_secondary":"COMM_CHANNEL_CODE",
        "target_business_key": "PARTY_MDM_ID",
        "target_business_key_secondary" : "COMM_CHANNEL_CODE",
        "hashCol" : ["OPT_IN_FLAG","COMM_CHANNEL_CODE","DEACTIVATEDRECORD"] ,
        "valid_from_col": "VALID_FROM",
        "batch_id":batch_id,
        "valid_to_col": "VALID_TO",
        "scd_type":"TYPE_2",
        "row_expansion": "COMMUNICATION_PRFERENCES",
        "attributes": ["PARTY_MDM_ID","OPT_IN_FLAG","COMM_CHANNEL_CODE" ,"VALID_FROM","VALID_TO","HASH_VALUE","DELETED_FLAG","BATCH_ID"],
        "attribute_logic": {
            "PARTY_MDM_ID": "ID",
            "OPT_IN_FLAG": "CASE WHEN LENGTH(TRIM(OPT_IN_FLAG)) = 0 OR OPT_IN_FLAG IS NULL THEN 'N' ELSE OPT_IN_FLAG END",
            "DELETED_FLAG": "CASE WHEN DEACTIVATEDRECORD = 'Yes' THEN 'Y' ELSE 'N' END",
            "COMM_CHANNEL_CODE":  "COMM_CHANNEL_CODE", 
        "VALID_FROM": """
			  CASE
    WHEN TRY_TO_TIMESTAMP(TRIM(VALID_FROM), 'yyyy-MM-dd HH:mm:ss.SSSSSS') IS NOT NULL
        THEN TRY_TO_TIMESTAMP(TRIM(VALID_FROM), 'yyyy-MM-dd HH:mm:ss.SSSSSS')
    
    WHEN TRY_TO_TIMESTAMP(TRIM(VALID_FROM), 'yyyy-MM-dd HH:mm:ss.SSS') IS NOT NULL
        THEN TRY_TO_TIMESTAMP(TRIM(VALID_FROM), 'yyyy-MM-dd HH:mm:ss.SSS')
     
    WHEN TRY_TO_TIMESTAMP(TRIM(VALID_FROM), 'yyyy-MM-dd HH:mm:ss') IS NOT NULL
        THEN TRY_TO_TIMESTAMP(TRIM(VALID_FROM), 'yyyy-MM-dd HH:mm:ss')
    
    WHEN TRY_TO_TIMESTAMP(TRIM(VALID_FROM), 'dd/MM/yyyy HH:mm:ss.SSSSSS') IS NOT NULL
        THEN TRY_TO_TIMESTAMP(TRIM(VALID_FROM), 'dd/MM/yyyy HH:mm:ss.SSSSSS')
    
    WHEN TRY_TO_TIMESTAMP(TRIM(VALID_FROM), 'dd/MM/yyyy HH:mm:ss.SSS') IS NOT NULL
        THEN TRY_TO_TIMESTAMP(TRIM(VALID_FROM), 'dd/MM/yyyy HH:mm:ss.SSS')
        
    WHEN TRY_TO_TIMESTAMP(TRIM(VALID_FROM), 'dd/MM/yyyy HH:mm:ss') IS NOT NULL
        THEN TRY_TO_TIMESTAMP(TRIM(VALID_FROM), 'dd/MM/yyyy HH:mm:ss')
    
    ELSE RELATIONSHIPSTARTDATE
END
              """,
    
                 "VALID_TO": """
    CASE 
        WHEN DEACTIVATEDRECORD = 'Yes' THEN 
            CASE
                WHEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'yyyy-MM-dd HH:mm:ss.SSSSSS') IS NOT NULL THEN
                    TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'yyyy-MM-dd HH:mm:ss.SSSSSS')

                WHEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'yyyy-MM-dd HH:mm:ss.SSS') IS NOT NULL THEN
                    TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'yyyy-MM-dd HH:mm:ss.SSS')

                WHEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'yyyy-MM-dd HH:mm:ss') IS NOT NULL THEN
                    TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'yyyy-MM-dd HH:mm:ss')

                WHEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'dd/MM/yyyy HH:mm:ss.SSSSSS') IS NOT NULL THEN
                    TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'dd/MM/yyyy HH:mm:ss.SSSSSS')

                WHEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'dd/MM/yyyy HH:mm:ss.SSS') IS NOT NULL THEN
                    TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'dd/MM/yyyy HH:mm:ss.SSS')

                WHEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'dd/MM/yyyy HH:mm:ss') IS NOT NULL THEN
                    TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'dd/MM/yyyy HH:mm:ss')
            END
        ELSE 
            NULL 
      END
      """
        },
        "merge_condition_template": (
    "target.{target_business_key} = source_transformed.{business_key} "
    "AND target.{valid_to_col} IS NULL "
    "AND target.{target_business_key_secondary} = source_transformed.{business_key_secondary} "
),

"change_condition": """
source_transformed.RULE_VALIDATION_RESULT = 'PASS'
AND source_transformed.CASTING_VALIDATION_RESULT = 'PASS'
AND target.HASH_VALUE <> source_transformed.HASH_VALUE_transformed
""",
"insert_merge_condition_template": (
    "target.{target_business_key} = source_transformed.{business_key} "
    "AND target.{target_business_key_secondary} = source_transformed.{business_key_secondary} "
    "AND target.HASH_VALUE = source_transformed.HASH_VALUE_transformed"
),
                "insert_condition": (
    "source_transformed.RULE_VALIDATION_RESULT = 'PASS' "
    "AND source_transformed.CASTING_VALIDATION_RESULT = 'PASS' "
    "AND source_transformed.OPT_IN_FLAG IS NOT NULL"
),
        "surrogate_key": {"column": "SYSID"}
    }

]



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

func_code = """
from pyspark.sql.functions import (
    array, struct, col, explode, trim, length, when, lit
)

def normalize_comm_preference_rows(source_df):

    return (
        source_df.withColumn(
            "comm_pref_array",
            array(
                struct(
                    col("AGMEMAILOPTIN_OPTINVALUE").alias("OPT_IN_FLAG"),
                    when(
                        length(trim(col("AGMEMAILOPTIN_LASTEDITDATEAGMEMAILOPT"))) == 0,
                        None
                    ).otherwise(col("AGMEMAILOPTIN_LASTEDITDATEAGMEMAILOPT")).alias("VALID_FROM"),
                    lit("AGM").alias("COMM_CHANNEL_CODE")
                ),

                struct(
                    col("EMAILOPTIN_OPTINVALUE").alias("OPT_IN_FLAG"),
                    when(
                        length(trim(col("EMAILOPTIN_LASTEDITDATEEMAILOPT"))) == 0,
                        None
                    ).otherwise(col("EMAILOPTIN_LASTEDITDATEEMAILOPT")).alias("VALID_FROM"),
                    lit("EMAIL").alias("COMM_CHANNEL_CODE")
                ),

                struct(
                    col("MAILOPTIN_OPTINVALUE").alias("OPT_IN_FLAG"),
                    when(
                        length(trim(col("MAILOPTIN_LASTEDITDATEMAILOPT"))) == 0,
                        None
                    ).otherwise(col("MAILOPTIN_LASTEDITDATEMAILOPT")).alias("VALID_FROM"),
                    lit("POST").alias("COMM_CHANNEL_CODE")
                ),

                struct(
                    col("TELOPTIN_OPTINVALUE").alias("OPT_IN_FLAG"),
                    when(
                        length(trim(col("TELOPTIN_LASTEDITDATETELOPT"))) == 0,
                        None
                    ).otherwise(col("TELOPTIN_LASTEDITDATETELOPT")).alias("VALID_FROM"),
                    lit("TEL").alias("COMM_CHANNEL_CODE")
                )
            )
        )
        .withColumn("comm_pref_struct", explode("comm_pref_array"))
        .withColumn("OPT_IN_FLAG", col("comm_pref_struct.OPT_IN_FLAG"))
        .withColumn("COMM_CHANNEL_CODE", col("comm_pref_struct.COMM_CHANNEL_CODE"))
        .withColumn("VALID_FROM", col("comm_pref_struct.VALID_FROM"))
        .drop("comm_pref_array", "comm_pref_struct")
    )
       """

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
mssparkutils.notebook.exit(json.dumps({ "tables_config": tables_config, "func_code": func_code}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
