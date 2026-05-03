# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

def safe_ancient_ts(parse_expr: str) -> str:
    return f"""
    CASE
        WHEN ({parse_expr}) IS NULL THEN NULL
        WHEN ({parse_expr}) < TIMESTAMP '1900-01-01 00:00:00'
            THEN TIMESTAMP '1900-01-02 00:00:00'
        ELSE ({parse_expr})
    END
    """
 
source_edit_date = """
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
    ELSE NULL
END
"""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables_config = [
    {
        "source_table": "LHDEVBRONZE.bronze_staging.customer_bronze_snapshot",
        "target_table": "LHDEVSILVER.silver.people",
        "business_key": "ID",
        "batch_id": batch_id,
        "target_business_key": "PARTY_MDM_ID",

        "hashCol": [
            "FIRST_NAMES", "LAST_NAME", "TITLE_CODE", "SUFFIX", "GENDER_CODE",
            "DATE_OF_BIRTH", "MARITAL_STATUS_CODE", "NATIONAL_INSURANCE_NUMBER",
            "TAX_ID_NUMBER", "TAX_DOMICILE_CODE", "RELATIONSHIP_START_DATE",
            "RELATIONSHIP_END_DATE", "DORMANT_FLAG", "DECEASED_DATE",
            "DECEASED_NOTIFICATION_DATE", "DECEASED_EVIDENCE_DATE",
            "MCNR_DEBT_FLAG", "OPEN_COMPLAINT_FLAG", "OPEN_LITIGATION_FLAG",
            "DELETED_FLAG","SOURCE_EDIT_DATE"
        ],

        "valid_from_col": "VALID_FROM",
        "valid_to_col": "VALID_TO",
        "scd_type": "TYPE_2",

        "attributes": [
            "PARTY_MDM_ID", "FIRST_NAMES", "LAST_NAME", "TITLE_CODE", "SUFFIX",
            "GENDER_CODE", "DATE_OF_BIRTH", "MARITAL_STATUS_CODE",
            "NATIONAL_INSURANCE_NUMBER", "TAX_ID_NUMBER", "TAX_DOMICILE_CODE",
            "RELATIONSHIP_START_DATE", "RELATIONSHIP_END_DATE", "DORMANT_FLAG",
            "DECEASED_DATE", "DECEASED_NOTIFICATION_DATE",
            "DECEASED_EVIDENCE_DATE", "MCNR_DEBT_FLAG", "OPEN_COMPLAINT_FLAG",
            "OPEN_LITIGATION_FLAG", "HASH_VALUE", "DELETED_FLAG","SOURCE_EDIT_DATE",
            "VALID_FROM", "VALID_TO", "BATCH_ID", "LOAD_DATE"
        ],

        "attribute_logic": {
            "PARTY_MDM_ID": "ID",
            "FIRST_NAMES": "FORENAMES",
            "LAST_NAME": "LASTNAME",
            "TITLE_CODE": "TITLE",
            "SUFFIX": "PERSONALNAMESUFFIX",
            "GENDER_CODE": "GENDER",

            "DATE_OF_BIRTH": """CASE 
                                    WHEN TRIM(BIRTHDATE) IS NULL 
                                         OR LENGTH(TRIM(BIRTHDATE)) = 0
                                        THEN NULL
                                    WHEN TO_DATE(BIRTHDATE) <= DATE '1900-01-01'
                                        THEN DATE '1900-01-02'
                                    ELSE TO_DATE(BIRTHDATE)
                                END
                                """,

            "MARITAL_STATUS_CODE": "MARITALSTATUS",
            "NATIONAL_INSURANCE_NUMBER": "NATIONALINSURANCENUMBER",
            "TAX_ID_NUMBER": "TAXIDNUMBER",
            "TAX_DOMICILE_CODE": "TAXDOMICILE",

            "RELATIONSHIP_START_DATE": """CASE 
                                             WHEN TRIM(RELATIONSHIPSTARTDATE) IS NULL 
                                                  OR LENGTH(TRIM(RELATIONSHIPSTARTDATE)) = 0
                                                 THEN NULL
                                             WHEN TO_DATE(RELATIONSHIPSTARTDATE) <= DATE '1900-01-01'
                                                 THEN DATE '1900-01-02'
                                             ELSE TO_DATE(RELATIONSHIPSTARTDATE)
                                         END
                                         """,

            "RELATIONSHIP_END_DATE": """CASE 
                                           WHEN TRIM(RELATIONSHIPENDDATE) IS NULL 
                                                OR LENGTH(TRIM(RELATIONSHIPENDDATE)) = 0
                                               THEN NULL
                                           WHEN TO_DATE(RELATIONSHIPENDDATE) <= DATE '1900-01-01'
                                               THEN DATE '1900-01-02'
                                           ELSE TO_DATE(RELATIONSHIPENDDATE)
                                       END
                                       """,

            "DORMANT_FLAG": "DORMANTACCOUNTFLAG",

            "DECEASED_DATE": """CASE 
                                    WHEN TRIM(DECEASEDDATE) IS NULL 
                                         OR LENGTH(TRIM(DECEASEDDATE)) = 0
                                        THEN NULL
                                    WHEN TO_DATE(DECEASEDDATE) <= DATE '1900-01-01'
                                        THEN DATE '1900-01-02'
                                    ELSE TO_DATE(DECEASEDDATE)
                                END
                                """,

            "DECEASED_NOTIFICATION_DATE": """CASE 
                                                WHEN TRIM(DECEASEDNOTIFICATIONDATE) IS NULL 
                                                     OR LENGTH(TRIM(DECEASEDNOTIFICATIONDATE)) = 0
                                                    THEN NULL
                                                WHEN TO_DATE(DECEASEDNOTIFICATIONDATE) <= DATE '1900-01-01'
                                                    THEN DATE '1900-01-02'
                                                ELSE TO_DATE(DECEASEDNOTIFICATIONDATE)
                                            END
                                            """,

            "DECEASED_EVIDENCE_DATE": """CASE 
                                            WHEN TRIM(DEATHEVIDENCESEENDATE) IS NULL 
                                                 OR LENGTH(TRIM(DEATHEVIDENCESEENDATE)) = 0
                                                THEN NULL
                                            WHEN TO_DATE(DEATHEVIDENCESEENDATE) <= DATE '1900-01-01'
                                                THEN DATE '1900-01-02'
                                            ELSE TO_DATE(DEATHEVIDENCESEENDATE)
                                        END
                                        """,

            "MCNR_DEBT_FLAG": "MCNRFLAG",
            "OPEN_COMPLAINT_FLAG": "OPENCOMPLAINTFLAG",
            "OPEN_LITIGATION_FLAG": "OPENLITIGATIONFLAG",

            "DELETED_FLAG": """
                CASE 
                    WHEN UPPER(TRIM(COALESCE(DEACTIVATEDRECORD, 'No'))) = 'YES' 
                        THEN 'Y' 
                    ELSE 'N' 
                END
            """,
            "SOURCE_EDIT_DATE": f"""
               {safe_ancient_ts(source_edit_date)}                   
            """,
            "LOAD_DATE": "current_timestamp()", 

            "VALID_FROM": "current_timestamp()",

            "VALID_TO": f"""
                CASE
                    WHEN UPPER(TRIM(COALESCE(DEACTIVATEDRECORD, 'No'))) = 'YES'
                         THEN  current_timestamp()
                    ELSE NULL
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
