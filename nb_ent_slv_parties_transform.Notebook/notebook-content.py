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
        "target_table": "LHDEVSILVER.silver.parties",
        "business_key": "ID",
        "target_business_key": "PARTY_MDM_ID",
        "row_expansion": "Not Applicable",
        "hashCol" : ["PARTY_MDM_ID","STATUS","DELETED_FLAG"] ,
        "valid_from_col": "INSERTED_DATE",
        "updated_col": "UPDATED_DATE",
        "scd_type":"TYPE_1",
        "batch_id":batch_id,
        "attributes": ["PARTY_MDM_ID","STATUS","HASH_VALUE","INSERTED_DATE","DELETED_FLAG","BATCH_ID"],
        "attribute_logic": {
            "PARTY_MDM_ID": "ID",
            "STATUS": "CASE WHEN RELATIONSHIPENDDATE IS NULL THEN 'ACTIVE' ELSE 'INACTIVE' END",
             "DELETED_FLAG": "CASE WHEN UPPER(TRIM(COALESCE(DEACTIVATEDRECORD, 'No'))) = 'YES' THEN 'Y' ELSE 'N' END",
            "INSERTED_DATE":"current_timestamp()"
        },
    "merge_condition_template": (
    "target.{target_business_key} = source_transformed.{business_key} "
),
"insert_merge_condition_template": (
    "target.{target_business_key} = source_transformed.{business_key} "
    "AND target.HASH_VALUE = source_transformed.HASH_VALUE_transformed"
),

"change_condition": """
source_transformed.RULE_VALIDATION_RESULT = 'PASS'
AND source_transformed.CASTING_VALIDATION_RESULT = 'PASS'
AND COALESCE(target.HASH_VALUE, '') <> COALESCE(source_transformed.HASH_VALUE_transformed, '')
""",
                        "insert_condition": (
    "source_transformed.RULE_VALIDATION_RESULT = 'PASS' "
    "AND source_transformed.CASTING_VALIDATION_RESULT = 'PASS'"
),
        "surrogate_key": None
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
