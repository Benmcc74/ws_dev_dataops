# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_interface (INTERFACE_ID, INTERFACE_NAME)
# MAGIC VALUES ('INTERFACE_01', 'STIBO');


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO control.cfg_bronze_raw (
# MAGIC     BRONZE_RAW_ENTITY_ID,
# MAGIC     INTERFACE_ID,
# MAGIC     SOURCE_TYPE,
# MAGIC     FABRIC_PIPELINE_NAME,
# MAGIC     SOURCE_FILE_PATH,
# MAGIC     FILE_NAME,
# MAGIC     TARGET_FILE_PATH,
# MAGIC     ISACTIVE
# MAGIC )
# MAGIC VALUES
# MAGIC     (
# MAGIC         1,
# MAGIC         'INTERFACE_01',
# MAGIC         'FILE',
# MAGIC         'adf_pl_stibo_integration_copy',
# MAGIC         '/upload/server-side-delivery/MDPOutbound',
# MAGIC         'Customer_Stibo_Delta_YYYYMMDDHHMMSS.csv',
# MAGIC         'Bronze/Raw/Stibo/Customer/Source/Daily/YYYY/MM/DD/',
# MAGIC         'Y'
# MAGIC     ),
# MAGIC     (
# MAGIC         2,
# MAGIC         'INTERFACE_01',
# MAGIC          'FILE',
# MAGIC          'adf_pl_stibo_integration_copy',
# MAGIC         '/upload/server-side-delivery/MDPOutbound',
# MAGIC         'Customer_Stibo_InitialLoad_YYYYMMDDHHMMSS.csv',
# MAGIC         'Bronze/Raw/Stibo/Customer/Source/InitialLoad/YYYY/MM/DD/',
# MAGIC         'Y'
# MAGIC     );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO control.cfg_bronze_staging (
# MAGIC BRONZE_STAGING_ENTITY_ID,
# MAGIC INTERFACE_ID,
# MAGIC SOURCE_SYSTEM,
# MAGIC SOURCE_TYPE,
# MAGIC SOURCE_PATH,
# MAGIC PROCESSED_PATH,
# MAGIC QUARANTINE_PATH,
# MAGIC ROOT_SOURCE_PATH,
# MAGIC ROOT_PROCESSED_PATH,
# MAGIC ROOT_QUARANTINE_PATH,
# MAGIC RETENTION_DAYS,
# MAGIC SOURCE_OBJECT_NAME,
# MAGIC SOURCE_FILE_PATTERN,
# MAGIC SOURCE_FORMAT,
# MAGIC SOURCE_FILE_DELIMITER,
# MAGIC SOURCE_FILE_HEADER,
# MAGIC SOURCE_FILE_COMPRESSION,
# MAGIC FILE_SIZE_MIN_BYTES,
# MAGIC KEY_COLUMN,
# MAGIC DELETE_COLUMN,
# MAGIC DELETE_COLUMN_VALUE,
# MAGIC TARGET_FORMAT,
# MAGIC TARGET_LAKEHOUSE,
# MAGIC TARGET_SCHEMA,
# MAGIC TARGET_TABLE_NAME,
# MAGIC TARGET_WRITE_MODE,
# MAGIC HISTORY_LOAD_INDICATOR,
# MAGIC HISTORY_TARGET_FORMAT,
# MAGIC HISTORY_TARGET_SCHEMA,
# MAGIC HISTORY_TARGET_TABLE_NAME,
# MAGIC HISTORY_TARGET_WRITE_MODE,
# MAGIC RULE_VALIDATION_RESULTS_TABLE_NAME,   
# MAGIC SCHEMA_VALIDATION_RESULTS_TABLE_NAME,
# MAGIC RECORD_IDENTIFIER_COLUMN,             
# MAGIC RECORD_IDENTIFIER_VALUE_COLUMN, 
# MAGIC VALIDATION_SOURCE_COLUMN,
# MAGIC ISACTIVE
# MAGIC ) VALUES (
# MAGIC     1,
# MAGIC     'INTERFACE_01',
# MAGIC     'STIBO',
# MAGIC     'File',
# MAGIC     'Files/Bronze/Raw/Stibo/Customer/Source/Daily/YYYY/MM/DD/',
# MAGIC     'Files/Bronze/Raw/Stibo/Customer/Processed/Daily/YYYY/MM/DD/',
# MAGIC     'Files/Bronze/Raw/Stibo/Customer/Quarantine/Daily/YYYY/MM/DD/',
# MAGIC     'Files/Bronze/Raw/Stibo/Customer/Source/',
# MAGIC     'Files/Bronze/Raw/Stibo/Customer/Processed/',
# MAGIC     'Files/Bronze/Raw/Stibo/Customer/Quarantine/',
# MAGIC     1,
# MAGIC     'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv',
# MAGIC     '*.csv',
# MAGIC     'CSV',
# MAGIC     '',
# MAGIC     'TRUE',
# MAGIC     'NONE',
# MAGIC     '',
# MAGIC     'ID',
# MAGIC     'DELETEDINCORE',
# MAGIC     'Y',
# MAGIC     'delta',
# MAGIC     'LHDEVBRONZE',
# MAGIC     'bronze_staging',
# MAGIC     'customer_bronze_snapshot',
# MAGIC     'OVERWRITE',
# MAGIC     'Y',
# MAGIC      'delta',
# MAGIC     'bronze_staging',
# MAGIC     'customer_bronze_history',
# MAGIC     'APPEND',
# MAGIC     'LHDEVBRONZE.audit.ops_validation_results',
# MAGIC     'LHDEVBRONZE.audit.ops_schema_validation_results',
# MAGIC     'RECORD_IDENTIFIER',
# MAGIC     'RECORD_IDENTIFIER_VALUE',
# MAGIC     'SOURCE_NAME',
# MAGIC     'Y'
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO control.cfg_bronze_staging (
# MAGIC BRONZE_STAGING_ENTITY_ID,
# MAGIC INTERFACE_ID,
# MAGIC SOURCE_SYSTEM,
# MAGIC SOURCE_TYPE,
# MAGIC SOURCE_PATH,
# MAGIC PROCESSED_PATH,
# MAGIC QUARANTINE_PATH,
# MAGIC ROOT_SOURCE_PATH,
# MAGIC ROOT_PROCESSED_PATH,
# MAGIC ROOT_QUARANTINE_PATH,
# MAGIC RETENTION_DAYS,
# MAGIC SOURCE_OBJECT_NAME,
# MAGIC SOURCE_FILE_PATTERN,
# MAGIC SOURCE_FORMAT,
# MAGIC SOURCE_FILE_DELIMITER,
# MAGIC SOURCE_FILE_HEADER,
# MAGIC SOURCE_FILE_COMPRESSION,
# MAGIC FILE_SIZE_MIN_BYTES,
# MAGIC KEY_COLUMN,
# MAGIC DELETE_COLUMN,
# MAGIC DELETE_COLUMN_VALUE,
# MAGIC TARGET_FORMAT,
# MAGIC TARGET_LAKEHOUSE,
# MAGIC TARGET_SCHEMA,
# MAGIC TARGET_TABLE_NAME,
# MAGIC TARGET_WRITE_MODE,
# MAGIC HISTORY_LOAD_INDICATOR,
# MAGIC HISTORY_TARGET_FORMAT,
# MAGIC HISTORY_TARGET_SCHEMA,
# MAGIC HISTORY_TARGET_TABLE_NAME,
# MAGIC HISTORY_TARGET_WRITE_MODE,
# MAGIC RULE_VALIDATION_RESULTS_TABLE_NAME,   
# MAGIC SCHEMA_VALIDATION_RESULTS_TABLE_NAME,
# MAGIC RECORD_IDENTIFIER_COLUMN,             
# MAGIC RECORD_IDENTIFIER_VALUE_COLUMN, 
# MAGIC VALIDATION_SOURCE_COLUMN,
# MAGIC ISACTIVE
# MAGIC ) VALUES (
# MAGIC     2,
# MAGIC     'INTERFACE_01',
# MAGIC     'STIBO',
# MAGIC     'File',
# MAGIC     'Files/Bronze/Raw/Stibo/Customer/Source/InitialLoad/YYYY/MM/DD/',
# MAGIC     'Files/Bronze/Raw/Stibo/Customer/Processed/InitialLoad/YYYY/MM/DD/',
# MAGIC     'Files/Bronze/Raw/Stibo/Customer/Quarantine/InitialLoad/YYYY/MM/DD/',
# MAGIC     'Files/Bronze/Raw/Stibo/Customer/Source/',
# MAGIC     'Files/Bronze/Raw/Stibo/Customer/Processed/',
# MAGIC     'Files/Bronze/Raw/Stibo/Customer/Quarantine/',
# MAGIC     1,
# MAGIC     'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv',
# MAGIC     '*.csv',
# MAGIC     'CSV',
# MAGIC     '',
# MAGIC     'TRUE',
# MAGIC     'NONE',
# MAGIC      '',
# MAGIC     'ID',
# MAGIC     'DELETEDINCORE',
# MAGIC     'Y',
# MAGIC     'delta',
# MAGIC     'LHDEVBRONZE',
# MAGIC     'bronze_staging',
# MAGIC     'customer_bronze_snapshot',
# MAGIC     'OVERWRITE',
# MAGIC     'Y',
# MAGIC      'delta',
# MAGIC     'bronze_staging',
# MAGIC     'customer_bronze_history',
# MAGIC     'APPEND',
# MAGIC     'LHDEVBRONZE.audit.ops_validation_results',
# MAGIC     'LHDEVBRONZE.audit.ops_schema_validation_results',
# MAGIC     'RECORD_IDENTIFIER',
# MAGIC     'RECORD_IDENTIFIER_VALUE',
# MAGIC     'SOURCE_NAME',
# MAGIC     'N'
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

	%%sql
INSERT INTO LHDEVBRONZE.control.cfg_silver (
    SILVER_ENTITY_ID,
    INTERFACE_ID,
    TABLE_NAME,
    EXECUTION_ORDER,
    NOTEBOOK_NAME,
    DELETE_KEY_COLUMN,
    DELETE_SRC_TABLE,
    DELETE_SRC_KEY_COLUMN,
    DELETE_SRC_COLUMN,
    DELETE_SRC_COLUMN_VALUE,
    ISACTIVE
)
VALUES
    (1,  'INTERFACE_01','LHDEVSILVER.silver.parties',1,'nb_ent_slv_parties_transform','PARTY_MDM_ID','LHDEVBRONZE.bronze_staging.customer_bronze_snapshot','ID','DELETEDINCORE','Y', 'Y'),
    (2,  'INTERFACE_01','LHDEVSILVER.silver.party_references',2,'nb_ent_slv_party_references_transform','PARTY_MDM_ID', 'LHDEVBRONZE.bronze_staging.customer_bronze_snapshot','ID','DELETEDINCORE','Y', 'Y'),
    (3,  'INTERFACE_01','LHDEVSILVER.silver.email_addresses',3,'nb_ent_slv_email_addresses_transform','PARTY_MDM_ID', 'LHDEVBRONZE.bronze_staging.customer_bronze_snapshot','ID','DELETEDINCORE','Y', 'Y'),
    (4,  'INTERFACE_01','LHDEVSILVER.silver.phone_numbers',4,'nb_ent_slv_phone_numbers_transform','PARTY_MDM_ID','LHDEVBRONZE.bronze_staging.customer_bronze_snapshot','ID','DELETEDINCORE','Y', 'Y'),
	(5,  'INTERFACE_01','LHDEVSILVER.silver.postal_addresses',5,'nb_ent_slv_postal_addresses_transform','PARTY_MDM_ID', 'LHDEVBRONZE.bronze_staging.customer_bronze_snapshot','ID','DELETEDINCORE','Y', 'Y'),
    (6,  'INTERFACE_01','LHDEVSILVER.silver.communication_preferences',6,'nb_ent_slv_communication_preferences_transform', 'PARTY_MDM_ID','LHDEVBRONZE.bronze_staging.customer_bronze_snapshot','ID','DELETEDINCORE','Y', 'Y'),
    (7,  'INTERFACE_01','LHDEVSILVER.silver.party_nationalities',7,'nb_ent_slv_party_nationalities_transform','PARTY_MDM_ID', 'LHDEVBRONZE.bronze_staging.customer_bronze_snapshot','ID','DELETEDINCORE','Y', 'Y'),
    (8,  'INTERFACE_01','LHDEVSILVER.silver.people',8,'nb_ent_slv_people_transform', 'PARTY_MDM_ID','LHDEVBRONZE.bronze_staging.customer_bronze_snapshot','ID','DELETEDINCORE','Y', 'Y');

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_gold (
# MAGIC     GOLD_ENTITY_ID,
# MAGIC     INTERFACE_ID,
# MAGIC     TABLE_NAME,
# MAGIC     EXECUTION_ORDER,
# MAGIC     STORED_PROCEDURE_NAME,
# MAGIC     ISACTIVE
# MAGIC )
# MAGIC VALUES
# MAGIC     (1,  'INTERFACE_01','gold.DIM_PARTY_CONTACT',1,'sp_DIM_PARTY_CONTACT', 'Y'),
# MAGIC     (2,  'INTERFACE_01','gold.DIM_PARTY_NATIONALITY',2,'sp_PARTY_NATIONALITY', 'Y'),
# MAGIC     (3,  'INTERFACE_01','gold.DIM_PARTY_DETAIL',3,'sp_PARTY_DETAIL', 'Y'),
# MAGIC     (4,  'INTERFACE_01','gold.DIM_PARTY_FLAG',4,'sp_PARTY_FLAG', 'Y'),
# MAGIC     (5,  'INTERFACE_01','gold.BRIDGE_PARTY_NATIONALITY',5,'sp_BRIDGE_PARTY_NATIONALITY', 'Y'),
# MAGIC 	(6,  'INTERFACE_01','gold.PARTY_DEMOGRAPHICS_FACTLESS',6,'sp_PARTY_DEMOGRAPHICS_FACTLESS', 'Y');

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (1, 'INTERFACE_01','Empty Value Check','bronze_staging.customer_bronze_snapshot', 'ID','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (2, 'INTERFACE_01','Duplicate Check','bronze_staging.customer_bronze_snapshot', 'ID','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (3, 'INTERFACE_01','Null Value Check','bronze_staging.customer_bronze_snapshot', 'ID','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (4, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (5, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'EMAIL_LASTEDITDATEEMAIL','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (6, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'HOMEPHONE_LASTEDITDATEHOME','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (7, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'MOBILEPHONE_LASTEDITDATEMOBILE','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (8, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'WORKPHONE_LASTEDITDATEWORK','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (9, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'AGMEMAILOPTIN_LASTEDITDATEAGMEMAILOPT','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (10, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'EMAILOPTIN_LASTEDITDATEEMAILOPT','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (11, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'MAILOPTIN_LASTEDITDATEMAILOPT','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (12, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'TELOPTIN_LASTEDITDATETELOPT','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (13, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'BIRTHDATE','ID', 'Low','Y');
# MAGIC 
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (14, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'RELATIONSHIPSTARTDATE','ID', 'Low','Y');
# MAGIC 
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (15, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'RELATIONSHIPENDDATE','ID', 'Low','Y');
# MAGIC 
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (16, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'DECEASEDDATE','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (17, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'DECEASEDNOTIFICATIONDATE','ID', 'Low','Y');
# MAGIC 
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (18, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'DEATHEVIDENCESEENDATE','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (19, 'INTERFACE_01','Spark Date Threshold Check','bronze_staging.customer_bronze_snapshot', 'LASTEDITDATEINDIVIDUALCUSTOMER','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, CONDITION_COLUMN,TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (20, 'INTERFACE_01','Conditional Null Check','bronze_staging.customer_bronze_snapshot', 'CORRESPONDENCEADDRESS_ADDRESSLINE1','CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, CONDITION_COLUMN,TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (21, 'INTERFACE_01','Conditional Null Check','bronze_staging.customer_bronze_snapshot', 'EMAIL_EMAIL','EMAIL_LASTEDITDATEEMAIL','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, CONDITION_COLUMN,TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (22, 'INTERFACE_01','Conditional Null Check','bronze_staging.customer_bronze_snapshot', 'HOMEPHONE_PHONENUMBER','HOMEPHONE_LASTEDITDATEHOME','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, CONDITION_COLUMN,TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (23, 'INTERFACE_01','Conditional Null Check','bronze_staging.customer_bronze_snapshot', 'MOBILEPHONE_PHONENUMBER','MOBILEPHONE_LASTEDITDATEMOBILE','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, CONDITION_COLUMN,TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (24, 'INTERFACE_01','Conditional Null Check','bronze_staging.customer_bronze_snapshot', 'WORKPHONE_PHONENUMBER','WORKPHONE_LASTEDITDATEWORK','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, CONDITION_COLUMN,TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (25, 'INTERFACE_01','Conditional Null Check','bronze_staging.customer_bronze_snapshot', 'ID','LASTEDITDATEINDIVIDUALCUSTOMER','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (26, 'INTERFACE_01','Null Value Check','bronze_staging.customer_bronze_snapshot', 'SOURCERECORDID','ID', 'Low','Y');
# MAGIC 
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_validation_rules_assignment (RULE_ASSIGNMENT_ID, INTERFACE_ID, RULE_TYPE, TABLE_NAME, TARGET_COLUMN, RECORD_IDENTIFIER, SEVERITY_TYPE,IS_ACTIVE)
# MAGIC VALUES (27, 'INTERFACE_01','Empty Value Check','bronze_staging.customer_bronze_snapshot', 'SOURCERECORDID','ID',  'Low','Y');

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_expected_schema (interface_id,active_flag,file_name,source_table_name, column_position, column_name, record_identifier)
# MAGIC VALUES
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 1, 'ID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 2, 'EVENTTYPE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 3, 'SOURCERECORDID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 4, 'SOURCESYSTEMID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 5, 'TITLE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 6, 'FORENAMES','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 7, 'LASTNAME','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 8, 'PERSONALNAMESUFFIX','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 9, 'BIRTHDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 10,'GENDER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 11, 'NATIONALINSURANCENUMBER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 12, 'NATIONALITY','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 13, 'MARITALSTATUS','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 14, 'EXCLUDEFROMDEDUPE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 15, 'TAXIDNUMBER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 16, 'TAXDOMICILE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 17, 'DEATHEVIDENCESEENDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 18, 'DECEASEDDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 19, 'DECEASEDNOTIFICATIONDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 20, 'DEACTIVATEDRECORD','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 21, 'RELATIONSHIPENDDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 22, 'RELATIONSHIPSTARTDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 23, 'SENTFORDELETIONDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 24, 'SENTFORDELETIONUSER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 25, 'DELETEDINCORE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 26, 'DELETEDINMDM','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 27, 'DELETIONDEDUPEEXCLUSION','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 28, 'DORMANTACCOUNTFLAG','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 29, 'MANUALEXCEPTIONFLAG','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 30, 'MANUALEXCEPTIONREASON','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 31, 'MANUALEXCEPTIONUSER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 32, 'MCNRFLAG','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 33, 'OPENCOMPLAINTFLAG','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 34, 'OPENLITIGATIONFLAG','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 35, 'LASTEDITDATEINDIVIDUALCUSTOMER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 36, 'HOMEPHONE_CORENONPOSTALADDRESSSYSID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 37, 'HOMEPHONE_PHONENUMBER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 38, 'HOMEPHONE_LASTEDITDATEHOME','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 39, 'HOMEPHONE_PHONETYPE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 40, 'WORKPHONE_CORENONPOSTALADDRESSSYSID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 41, 'WORKPHONE_LASTEDITDATEWORK','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 42, 'WORKPHONE_PHONENUMBER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 43, 'WORKPHONE_PHONETYPE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 44, 'MOBILEPHONE_CORENONPOSTALADDRESSSYSID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 45, 'MOBILEPHONE_LASTEDITDATEMOBILE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 46, 'MOBILEPHONE_PHONENUMBER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 47, 'MOBILEPHONE_PHONETYPE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 48, 'CORRESPONDENCEADDRESS_COREADDRESSSYSID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 49, 'CORRESPONDENCEADDRESS_ADDRESSLINE1','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 50, 'CORRESPONDENCEADDRESS_ADDRESSLINE2','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 51, 'CORRESPONDENCEADDRESS_ADDRESSLINE3','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 52, 'CORRESPONDENCEADDRESS_ADDRESSLINE4','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 53, 'CORRESPONDENCEADDRESS_ADDRESSLINE5','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 54, 'CORRESPONDENCEADDRESS_POSTCODE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 55, 'CORRESPONDENCEADDRESS_COUNTRYCODE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 56, 'CORRESPONDENCEADDRESS_ADDRESSTYPECODE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 57, 'CORRESPONDENCEADDRESS_PAFSTATUS','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 58, 'CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 59, 'EMAIL_CORENONPOSTALADDRESSSYSID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 60, 'EMAIL_EMAIL','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 61, 'EMAIL_LASTEMAILENDDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 62, 'EMAIL_LASTEDITDATEEMAIL','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 63, 'AGMEMAILOPTIN_OPTINVALUE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 64, 'AGMEMAILOPTIN_LASTEDITDATEAGMEMAILOPT','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 65, 'EMAILOPTIN_OPTINVALUE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 66, 'EMAILOPTIN_LASTEDITDATEEMAILOPT','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 67, 'MAILOPTIN_OPTINVALUE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 68, 'MAILOPTIN_LASTEDITDATEMAILOPT','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 69, 'TELOPTIN_OPTINVALUE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_Delta_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 70, 'TELOPTIN_LASTEDITDATETELOPT','ID');

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO LHDEVBRONZE.control.cfg_expected_schema (interface_id,active_flag,file_name,source_table_name, column_position, column_name, record_identifier)
# MAGIC VALUES
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 1, 'ID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 2, 'EVENTTYPE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 3, 'SOURCERECORDID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 4, 'SOURCESYSTEMID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 5, 'TITLE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 6, 'FORENAMES','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 7, 'LASTNAME','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 8, 'PERSONALNAMESUFFIX','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 9, 'BIRTHDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 10,'GENDER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 11, 'NATIONALINSURANCENUMBER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 12, 'NATIONALITY','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 13, 'MARITALSTATUS','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 14, 'EXCLUDEFROMDEDUPE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 15, 'TAXIDNUMBER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 16, 'TAXDOMICILE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 17, 'DEATHEVIDENCESEENDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 18, 'DECEASEDDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 19, 'DECEASEDNOTIFICATIONDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 20, 'DEACTIVATEDRECORD','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 21, 'RELATIONSHIPENDDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 22, 'RELATIONSHIPSTARTDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 23, 'SENTFORDELETIONDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 24, 'SENTFORDELETIONUSER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 25, 'DELETEDINCORE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 26, 'DELETEDINMDM','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 27, 'DELETIONDEDUPEEXCLUSION','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 28, 'DORMANTACCOUNTFLAG','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 29, 'MANUALEXCEPTIONFLAG','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 30, 'MANUALEXCEPTIONREASON','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 31, 'MANUALEXCEPTIONUSER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 32, 'MCNRFLAG','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 33, 'OPENCOMPLAINTFLAG','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 34, 'OPENLITIGATIONFLAG','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 35, 'LASTEDITDATEINDIVIDUALCUSTOMER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 36, 'HOMEPHONE_CORENONPOSTALADDRESSSYSID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 37, 'HOMEPHONE_PHONENUMBER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 38, 'HOMEPHONE_LASTEDITDATEHOME','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 39, 'HOMEPHONE_PHONETYPE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 40, 'WORKPHONE_CORENONPOSTALADDRESSSYSID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 41, 'WORKPHONE_LASTEDITDATEWORK','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 42, 'WORKPHONE_PHONENUMBER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 43, 'WORKPHONE_PHONETYPE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 44, 'MOBILEPHONE_CORENONPOSTALADDRESSSYSID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 45, 'MOBILEPHONE_LASTEDITDATEMOBILE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 46, 'MOBILEPHONE_PHONENUMBER','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 47, 'MOBILEPHONE_PHONETYPE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 48, 'CORRESPONDENCEADDRESS_COREADDRESSSYSID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 49, 'CORRESPONDENCEADDRESS_ADDRESSLINE1','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 50, 'CORRESPONDENCEADDRESS_ADDRESSLINE2','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 51, 'CORRESPONDENCEADDRESS_ADDRESSLINE3','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 52, 'CORRESPONDENCEADDRESS_ADDRESSLINE4','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 53, 'CORRESPONDENCEADDRESS_ADDRESSLINE5','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 54, 'CORRESPONDENCEADDRESS_POSTCODE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 55, 'CORRESPONDENCEADDRESS_COUNTRYCODE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 56, 'CORRESPONDENCEADDRESS_ADDRESSTYPECODE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 57, 'CORRESPONDENCEADDRESS_PAFSTATUS','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 58, 'CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 59, 'EMAIL_CORENONPOSTALADDRESSSYSID','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 60, 'EMAIL_EMAIL','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 61, 'EMAIL_LASTEMAILENDDATE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 62, 'EMAIL_LASTEDITDATEEMAIL','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 63, 'AGMEMAILOPTIN_OPTINVALUE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 64, 'AGMEMAILOPTIN_LASTEDITDATEAGMEMAILOPT','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 65, 'EMAILOPTIN_OPTINVALUE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 66, 'EMAILOPTIN_LASTEDITDATEEMAILOPT','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 67, 'MAILOPTIN_OPTINVALUE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 68, 'MAILOPTIN_LASTEDITDATEMAILOPT','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 69, 'TELOPTIN_OPTINVALUE','ID'),
# MAGIC ('INTERFACE_01', 'Y', 'Customer_Stibo_InitialLoad_YYYYMMDDhhmmss.csv', 'bronze_staging.customer_bronze_snapshot', 70, 'TELOPTIN_LASTEDITDATETELOPT','ID');

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'TIMESTAMP'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'AGMEMAILOPTIN_LASTEDITDATEAGMEMAILOPT';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'TIMESTAMP'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'EMAILOPTIN_LASTEDITDATEEMAILOPT';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'TIMESTAMP'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'MAILOPTIN_LASTEDITDATEMAILOPT';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'TIMESTAMP'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'TELOPTIN_LASTEDITDATETELOPT';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'TIMESTAMP'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'EMAIL_LASTEDITDATEEMAIL';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'TIMESTAMP'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'LASTEDITDATEINDIVIDUALCUSTOMER';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'DATE'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'BIRTHDATE';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'DATE'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'RELATIONSHIPSTARTDATE';
# MAGIC 
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'DATE'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'RELATIONSHIPENDDATE';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'DATE'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'DECEASEDDATE';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'DATE'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'DECEASEDNOTIFICATIONDATE';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'DATE'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'DEATHEVIDENCESEENDATE';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'TIMESTAMP'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'HOMEPHONE_LASTEDITDATEHOME';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'TIMESTAMP'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'MOBILEPHONE_LASTEDITDATEMOBILE';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'TIMESTAMP'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'WORKPHONE_LASTEDITDATEWORK';
# MAGIC  
# MAGIC UPDATE LHDEVBRONZE.control.cfg_expected_schema
# MAGIC SET data_type_validation = 'TIMESTAMP'
# MAGIC WHERE source_table_name = 'bronze_staging.customer_bronze_snapshot' and  column_name = 'CORRESPONDENCEADDRESS_LASTEDITDATECORRESPONDENCEADDRESS';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
