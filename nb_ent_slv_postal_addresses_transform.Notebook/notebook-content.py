# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import json

tables_config = [
    {   "source_table":"LHDEVBRONZE.bronze_staging.customer_bronze_snapshot",
        "target_table": "LHDEVSILVER.silver.postal_addresses",
        "business_key": "ID",
        "batch_id":batch_id,
        "target_business_key": "PARTY_MDM_ID",
        "hashCol" : ["ADDRESS_TYPE_CODE","LINE_1", "LINE_2", "LINE_3","LINE_4","LINE_5","COUNTRY_CODE","POSTCODE","PAF_STATUS","DELETED_FLAG"] ,
        "valid_from_col": "VALID_FROM",
        "valid_to_col": "VALID_TO",
        "scd_type":"TYPE_2",
        "attributes": ["PARTY_MDM_ID","ADDRESS_TYPE_CODE","LINE_1", "LINE_2", "LINE_3","LINE_4","LINE_5","COUNTRY_CODE","POSTCODE","PAF_STATUS","VALID_FROM","VALID_TO","HASH_VALUE","DELETED_FLAG","BATCH_ID"],
        "attribute_logic": {
            "PARTY_MDM_ID": "ID",
            "ADDRESS_TYPE_CODE":"CORRESPONDENCEADDRESS_ADDRESSTYPECODE",
            "LINE_1": "CORRESPONDENCEADDRESS_ADDRESSLINE1",
            "LINE_2": "CORRESPONDENCEADDRESS_ADDRESSLINE2",
            "LINE_3": "CORRESPONDENCEADDRESS_ADDRESSLINE3",
            "LINE_4": "CORRESPONDENCEADDRESS_ADDRESSLINE4",
            "LINE_5": "CORRESPONDENCEADDRESS_ADDRESSLINE5",
            "COUNTRY_CODE": "CORRESPONDENCEADDRESS_COUNTRYCODE",
            "POSTCODE": "CORRESPONDENCEADDRESS_POSTCODE",
            "PAF_STATUS": "CORRESPONDENCEADDRESS_PAFSTATUS",  
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
            WHEN TRY_TO_TIMESTAMP(TRIM(CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS), 'yyyy-MM-dd HH:mm:ss.SSSSSS') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS), 'yyyy-MM-dd HH:mm:ss.SSSSSS')

            WHEN TRY_TO_TIMESTAMP(TRIM(CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS), 'yyyy-MM-dd HH:mm:ss.SSS') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS), 'yyyy-MM-dd HH:mm:ss.SSS')

            WHEN TRY_TO_TIMESTAMP(TRIM(CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS), 'yyyy-MM-dd HH:mm:ss') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS), 'yyyy-MM-dd HH:mm:ss')

            WHEN TRY_TO_TIMESTAMP(TRIM(CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS), 'dd/MM/yyyy HH:mm:ss.SSSSSS') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS), 'dd/MM/yyyy HH:mm:ss.SSSSSS')

            WHEN TRY_TO_TIMESTAMP(TRIM(CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS), 'dd/MM/yyyy HH:mm:ss.SSS') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS), 'dd/MM/yyyy HH:mm:ss.SSS')

            WHEN TRY_TO_TIMESTAMP(TRIM(CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS), 'dd/MM/yyyy HH:mm:ss') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(TRIM(CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS), 'dd/MM/yyyy HH:mm:ss')
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
    "AND source_transformed.CORRESPONDENCEADDRESS_ADDRESSLINE1 IS NOT NULL"
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
