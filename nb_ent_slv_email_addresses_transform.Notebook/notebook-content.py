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
        "target_table": "LHDEVSILVER.silver.email_addresses",
        "business_key": "ID",
        "target_business_key": "PARTY_MDM_ID",
        "batch_id":batch_id,
        "hashCol" : ['EMAIL','DELETED_FLAG'] ,
        "valid_from_col": "VALID_FROM",
        "valid_to_col": "VALID_TO",
        "scd_type":"TYPE_2",
        "attributes": ["PARTY_MDM_ID","EMAIL", "VALID_FROM", "VALID_TO","HASH_VALUE","DELETED_FLAG","BATCH_ID"],
        "attribute_logic": {
            "PARTY_MDM_ID": "ID",
            "EMAIL": "EMAIL_EMAIL",
             "DELETED_FLAG": "CASE WHEN UPPER(TRIM(COALESCE(DEACTIVATEDRECORD, 'No'))) = 'YES' THEN 'Y' ELSE 'N' END",
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
            WHEN TRY_TO_TIMESTAMP(TRIM(EMAIL_LASTEDITDATEEMAIL), 'yyyy-MM-dd HH:mm:ss.SSSSSS') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(EMAIL_LASTEDITDATEEMAIL), 'yyyy-MM-dd HH:mm:ss.SSSSSS')

            WHEN TRY_TO_TIMESTAMP(TRIM(EMAIL_LASTEDITDATEEMAIL), 'yyyy-MM-dd HH:mm:ss.SSS') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(EMAIL_LASTEDITDATEEMAIL), 'yyyy-MM-dd HH:mm:ss.SSS')

            WHEN TRY_TO_TIMESTAMP(TRIM(EMAIL_LASTEDITDATEEMAIL), 'yyyy-MM-dd HH:mm:ss') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(EMAIL_LASTEDITDATEEMAIL), 'yyyy-MM-dd HH:mm:ss')

            WHEN TRY_TO_TIMESTAMP(TRIM(EMAIL_LASTEDITDATEEMAIL), 'dd/MM/yyyy HH:mm:ss.SSSSSS') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(EMAIL_LASTEDITDATEEMAIL), 'dd/MM/yyyy HH:mm:ss.SSSSSS')

            WHEN TRY_TO_TIMESTAMP(TRIM(EMAIL_LASTEDITDATEEMAIL), 'dd/MM/yyyy HH:mm:ss.SSS') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(EMAIL_LASTEDITDATEEMAIL), 'dd/MM/yyyy HH:mm:ss.SSS')

            WHEN TRY_TO_TIMESTAMP(TRIM(EMAIL_LASTEDITDATEEMAIL), 'dd/MM/yyyy HH:mm:ss') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(EMAIL_LASTEDITDATEEMAIL), 'dd/MM/yyyy HH:mm:ss')
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
),

"change_condition": """
source_transformed.RULE_VALIDATION_RESULT = 'PASS'
AND source_transformed.CASTING_VALIDATION_RESULT = 'PASS'
AND COALESCE(target.HASH_VALUE, '') <> COALESCE(source_transformed.HASH_VALUE_transformed, '')
""",
"insert_merge_condition_template": (
    "target.{target_business_key} = source_transformed.{business_key} "
    "AND target.{valid_to_col} IS NULL"
),
                "insert_condition": (
    "source_transformed.RULE_VALIDATION_RESULT = 'PASS' "
    "AND source_transformed.CASTING_VALIDATION_RESULT = 'PASS' "
    "AND source_transformed.EMAIL_EMAIL IS NOT NULL"
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

import json
mssparkutils.notebook.exit(json.dumps({ "tables_config": tables_config }))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
