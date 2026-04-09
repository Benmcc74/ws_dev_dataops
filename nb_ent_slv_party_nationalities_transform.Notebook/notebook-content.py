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
        "source_table":"LHDEVBRONZE.bronze_staging.customer_bronze_snapshot",
        "target_table": "LHDEVSILVER.silver.party_nationalities",
        "business_key": "ID",
        "target_business_key": "PARTY_MDM_ID",
        "batch_id":batch_id,
        "hashCol" : ["NATIONALITY_CODE","FIRST_DECLARED_NTNLTY_FLAG","DELETED_FLAG"] ,
        "valid_from_col": "VALID_FROM",
        "valid_to_col": "VALID_TO",
        "scd_type":"TYPE_2",
        "attributes": ["PARTY_MDM_ID","NATIONALITY_CODE","FIRST_DECLARED_NTNLTY_FLAG","VALID_FROM","VALID_TO","HASH_VALUE","DELETED_FLAG","BATCH_ID"],
        "attribute_logic": {
            "PARTY_MDM_ID": "ID",
            "NATIONALITY_CODE":"NATIONALITY",
             "DELETED_FLAG": "CASE WHEN UPPER(TRIM(COALESCE(DEACTIVATEDRECORD, 'No'))) = 'YES' THEN 'Y' ELSE 'N' END",
            "FIRST_DECLARED_NTNLTY_FLAG": "'Y'",  
                     "VALID_FROM": """
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
              """  ,    

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
"insert_merge_condition_template": (
    "target.{target_business_key} = source_transformed.{business_key} "
    "AND target.{valid_to_col} IS NULL"
),

"change_condition": """
source_transformed.RULE_VALIDATION_RESULT = 'PASS'
AND source_transformed.CASTING_VALIDATION_RESULT = 'PASS'
AND COALESCE(target.HASH_VALUE, '') <> COALESCE(source_transformed.HASH_VALUE_transformed, '')
""",
        "surrogate_key": {"column": "SYSID"},

        "insert_condition": (
    "source_transformed.RULE_VALIDATION_RESULT = 'PASS' "
    "AND source_transformed.CASTING_VALIDATION_RESULT = 'PASS' "
    "AND source_transformed.NATIONALITY IS NOT NULL"
)


    }

]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
mssparkutils.notebook.exit(json.dumps({ "tables_config": tables_config}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
