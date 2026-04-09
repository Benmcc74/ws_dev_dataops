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
        "target_table": "LHDEVSILVER.silver.phone_numbers",
        "business_key": "id",
        "batch_id":batch_id,
        "business_key_secondary":"PHONE_TYPE_CODE",
        "target_business_key": "PARTY_MDM_ID",
        "target_business_key_secondary" : "PHONE_TYPE_CODE",
        "hashCol" : ["PHONE_NUMBER","PHONE_TYPE_CODE","DELETED_FLAG"] ,
        "valid_from_col": "VALID_FROM",
        "valid_to_col": "VALID_TO",
        "scd_type":"TYPE_2",
        "row_expansion": "PHONE",
        "attributes": ["PARTY_MDM_ID","PHONE_NUMBER","PHONE_TYPE_CODE" ,"VALID_FROM","VALID_TO","HASH_VALUE","DELETED_FLAG","BATCH_ID"],
        "attribute_logic": {
            "PARTY_MDM_ID": "ID",
            "PHONE_NUMBER": "CASE WHEN LENGTH(TRIM(PHONE_NUMBER)) = 0 THEN NULL ELSE PHONE_NUMBER END",
            "DELETED_FLAG": "CASE WHEN UPPER(TRIM(COALESCE(DEACTIVATEDRECORD, 'No'))) = 'YES' THEN 'Y' ELSE 'N' END",
            "PHONE_TYPE_CODE":  "PHONE_TYPE_CODE", 
            "VALID_FROM": """
CASE
    WHEN DEACTIVATEDRECORD = 'Yes' THEN
        CASE
            WHEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'yyyy-MM-dd HH:mm:ss.SSSSSS') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'yyyy-MM-dd HH:mm:ss.SSSSSS')

            WHEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'yyyy-MM-dd HH:mm:ss.SSS') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'yyyy-MM-dd HH:mm:ss.SSS')

            WHEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'yyyy-MM-dd HH:mm:ss') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'yyyy-MM-dd HH:mm:ss')

            WHEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'dd/MM/yyyy HH:mm:ss.SSSSSS') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'dd/MM/yyyy HH:mm:ss.SSSSSS')

            WHEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'dd/MM/yyyy HH:mm:ss.SSS') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'dd/MM/yyyy HH:mm:ss.SSS')

            WHEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'dd/MM/yyyy HH:mm:ss') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(LASTEDITDATEINDIVIDUALCUSTOMER), 'dd/MM/yyyy HH:mm:ss')
        END
    ELSE
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
        END
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
"insert_merge_condition_template": (
    "target.{target_business_key} = source_transformed.{business_key} "
    "AND target.{target_business_key_secondary} = source_transformed.{business_key_secondary} "
    "AND target.{valid_to_col} IS NULL"
),

"change_condition": """
source_transformed.RULE_VALIDATION_RESULT = 'PASS'
AND source_transformed.CASTING_VALIDATION_RESULT = 'PASS'
AND COALESCE(target.HASH_VALUE, '') <> COALESCE(source_transformed.HASH_VALUE_transformed, '')
""",
                        "insert_condition": (
    "source_transformed.RULE_VALIDATION_RESULT = 'PASS' "
    "AND source_transformed.CASTING_VALIDATION_RESULT = 'PASS' "
    "AND source_transformed.PHONE_NUMBER IS NOT NULL"
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

def normalize_phone_rows(source_df):

    return (
        source_df.withColumn(
            "phone_array",
            array(
                struct(
                    col("HOMEPHONE_PHONENUMBER").alias("PHONE_NUMBER"),
                    when(
                        length(trim(col("HOMEPHONE_LASTEDITDATEHOME"))) == 0,
                        None
                    ).otherwise(col("HOMEPHONE_LASTEDITDATEHOME")).alias("VALID_FROM"),
                    lit("HOME").alias("PHONE_TYPE_CODE")
                ),

                struct(
                    col("MOBILEPHONE_PHONENUMBER").alias("PHONE_NUMBER"),
                    when(
                        length(trim(col("MOBILEPHONE_LASTEDITDATEMOBILE"))) == 0,
                        None
                    ).otherwise(col("MOBILEPHONE_LASTEDITDATEMOBILE")).alias("VALID_FROM"),
                    lit("MOBILE").alias("PHONE_TYPE_CODE")
                ),

                struct(
                    col("WORKPHONE_PHONENUMBER").alias("PHONE_NUMBER"),
                    when(
                        length(trim(col("WORKPHONE_LASTEDITDATEWORK"))) == 0,
                        None
                    ).otherwise(col("WORKPHONE_LASTEDITDATEWORK")).alias("VALID_FROM"),
                    lit("WORK").alias("PHONE_TYPE_CODE")
                )
            )
        )
        .withColumn("phone_struct", explode("phone_array"))
        .withColumn("PHONE_NUMBER", col("phone_struct.PHONE_NUMBER"))
        .withColumn("PHONE_TYPE_CODE", col("phone_struct.PHONE_TYPE_CODE"))
        .withColumn("VALID_FROM", col("phone_struct.VALID_FROM"))
        .drop("phone_array", "phone_struct")
    )

    """

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
mssparkutils.notebook.exit(json.dumps({ "tables_config": tables_config, "func_code": func_code }))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
