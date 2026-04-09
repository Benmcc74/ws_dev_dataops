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
        "target_table": "LHDEVSILVER.silver.people",
        "business_key": "ID",
        "batch_id":batch_id,
        "target_business_key": "PARTY_MDM_ID",
        "hashCol" : ["FIRST_NAMES", "LAST_NAME", "TITLE_CODE","SUFFIX","GENDER_CODE","DATE_OF_BIRTH","MARITAL_STATUS_CODE",
                      "NATIONAL_INSURANCE_NUMBER","TAX_ID_NUMBER","TAX_DOMICILE_CODE","RELATIONSHIP_START_DATE","RELATIONSHIP_END_DATE",
                     "DORMANT_FLAG","DECEASED_DATE","DECEASED_NOTIFICATION_DATE","DECEASED_EVIDENCE_DATE","MCNR_DEBT_FLAG","OPEN_COMPLAINT_FLAG","OPEN_LITIGATION_FLAG","DELETED_FLAG"] ,
        "valid_from_col": "VALID_FROM",
        "valid_to_col": "VALID_TO",
        "scd_type":"TYPE_2",
        "attributes": ["PARTY_MDM_ID","FIRST_NAMES","LAST_NAME", "TITLE_CODE", "SUFFIX","GENDER_CODE","DATE_OF_BIRTH","MARITAL_STATUS_CODE","NATIONAL_INSURANCE_NUMBER","TAX_ID_NUMBER","TAX_DOMICILE_CODE",
        "RELATIONSHIP_START_DATE","RELATIONSHIP_END_DATE","DORMANT_FLAG","DECEASED_DATE","DECEASED_NOTIFICATION_DATE","DECEASED_EVIDENCE_DATE","MCNR_DEBT_FLAG","OPEN_COMPLAINT_FLAG",
        "OPEN_LITIGATION_FLAG","HASH_VALUE","DELETED_FLAG","VALID_FROM","VALID_TO","BATCH_ID"],
        "attribute_logic": {
            "PARTY_MDM_ID": "ID",
            "FIRST_NAMES":"FORENAMES",
            "LAST_NAME": "LASTNAME",
            "TITLE_CODE": "TITLE",
            "SUFFIX": "PERSONALNAMESUFFIX",
            "GENDER_CODE": "GENDER",
            "DATE_OF_BIRTH": "TO_DATE(CASE WHEN LENGTH(TRIM(BIRTHDATE)) = 0 THEN NULL ELSE BIRTHDATE END)",
            "MARITAL_STATUS_CODE": "MARITALSTATUS",
            "NATIONAL_INSURANCE_NUMBER": "NATIONALINSURANCENUMBER",
            "TAX_ID_NUMBER": "TAXIDNUMBER",
            "TAX_DOMICILE_CODE": "TAXDOMICILE",            
            "RELATIONSHIP_START_DATE": "TO_DATE(CASE WHEN LENGTH(TRIM(RELATIONSHIPSTARTDATE)) = 0 THEN NULL ELSE RELATIONSHIPSTARTDATE END)",
            "RELATIONSHIP_END_DATE": "TO_DATE(CASE WHEN LENGTH(TRIM(RELATIONSHIPENDDATE)) = 0 THEN NULL ELSE RELATIONSHIPENDDATE END)",
            "DORMANT_FLAG": "DORMANTACCOUNTFLAG",
            "DECEASED_DATE":  "TO_DATE(CASE WHEN LENGTH(TRIM(DECEASEDDATE)) = 0 THEN NULL ELSE DECEASEDDATE END)",
            "DECEASED_NOTIFICATION_DATE": "TO_DATE(CASE WHEN LENGTH(TRIM(DECEASEDNOTIFICATIONDATE)) = 0 THEN NULL ELSE DECEASEDNOTIFICATIONDATE END)",
            "DECEASED_EVIDENCE_DATE": "TO_DATE(CASE WHEN LENGTH(TRIM(DEATHEVIDENCESEENDATE)) = 0 THEN NULL ELSE DEATHEVIDENCESEENDATE END)",
            "MCNR_DEBT_FLAG": "MCNRFLAG",
            "OPEN_COMPLAINT_FLAG": "OPENCOMPLAINTFLAG",
            "OPEN_LITIGATION_FLAG": "OPENLITIGATIONFLAG",
            "DELETED_FLAG": "CASE WHEN UPPER(TRIM(COALESCE(DEACTIVATEDRECORD, 'No'))) = 'YES' THEN 'Y' ELSE 'N' END",
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
    "AND source_transformed.CASTING_VALIDATION_RESULT = 'PASS'"
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
mssparkutils.notebook.exit(json.dumps({ "tables_config": tables_config}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
