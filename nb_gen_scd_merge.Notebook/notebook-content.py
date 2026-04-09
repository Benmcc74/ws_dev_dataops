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

print(f"Dispatcher received notebook name: {notebook_name}")
 
# Call the target notebook.
result = mssparkutils.notebook.run(
    notebook_name,  
    1800, 
    {"useRootDefaultLakehouse": True,"batch_id":batch_id}  # Parameters to child notebook.
)


result = json.loads(result)
tables_config = result["tables_config"]

if "func_code" in result:
 exec(result["func_code"])
 
import logging
from pyspark.sql.functions import (
    col, lit, sha2, concat_ws, coalesce, abs, rand, current_timestamp, expr
)
from delta.tables import DeltaTable
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)

audit_tbl = "LHDEVBRONZE.audit.ops_record_count_log"
# ---------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("SCD Framework for Silver Tables")


# ---------------------------------------------------------
# Helper functions 
# ---------------------------------------------------------

def preprocess_source(table_cfg, df):
    """
    Optional row expansion step based on table_cfg['row_expansion'].
    """
    if table_cfg.get("row_expansion") == "PHONE":
        return normalize_phone_rows(df)
    if table_cfg.get("row_expansion") == "COMMUNICATION_PRFERENCES":
        return normalize_comm_preference_rows(df)
    return df


def apply_transformations(df, attribute_logic):
    """
    Applies column-level transformations using Spark SQL expressions.
    Each transformed column is suffixed with '_transformed'.
    """
    for col_name, transform_expr in attribute_logic.items():
        df = df.withColumn(f"{col_name}_transformed", expr(transform_expr))
    return df


def validate_foreign_keys(source_df, table_cfg):
    """
    Validates foreign key constraints for a source DataFrame.

    table_cfg["foreign_keys"] format:
    [
      {
        "source_col": "col1",
        "parent_table": "dim_table",
        "parent_col": "id"
      },
      ...
    ]
    """
    if not table_cfg.get("foreign_keys"):
        return source_df

    for fk in table_cfg["foreign_keys"]:
        parent_df = (
            spark.read.table(fk["parent_table"])
            .select(col(fk["parent_col"]).alias("parent_key"))
            .dropDuplicates()
        )

        invalid_df = source_df.join(
            parent_df,
            source_df[fk["source_col"]] == parent_df["parent_key"],
            "left_anti"
        )

        if invalid_df.limit(1).count() > 0:
            violating_ids = [
                row[fk["source_col"]]
                for row in invalid_df.select(fk["source_col"]).distinct().limit(5).collect()
            ]
            raise Exception(
                f"Foreign key violation on {fk['source_col']} against {fk['parent_table']}. "
                f"Example violating keys: {violating_ids}"
            )

    return source_df


# ---------------------------------------------------------
# Config and schema validation helpers
# ---------------------------------------------------------

def validate_table_cfg(table_cfg):
    """
    Validates that required keys exist in table_cfg and have non‑empty values.
    Does NOT validate types or structure — only presence and non‑emptiness.
    """

    required_keys = [
        "source_table",
        "target_table",
        "attribute_logic",
        "attributes",
        "hashCol",
        "merge_condition_template",
        "change_condition",
        "valid_from_col"
    ]

    missing = []
    empty = []

    for key in required_keys:
        if key not in table_cfg:
            missing.append(key)
        else:
            # Check for empty values: None, empty string, empty list, empty dict
            value = table_cfg[key]
            if value is None or value == "" or value == [] or value == {}:
                empty.append(key)

    if missing:
        raise Exception(f"Missing required config keys: {missing}")

    if empty:
        raise Exception(f"Required config keys have empty values: {empty}")

    logger.info("table_cfg validation passed")




# ---------------------------------------------------------
# Dataframe preparation helpers
# ---------------------------------------------------------

def load_and_preprocess_source(table_cfg):
    """
    Loads the source table, handles optional row expansion, and checks for emptiness.
    Returns a DataFrame, or None if the source is empty.
    """
    logger.info(f"Loading source table: {table_cfg['source_table']}")
    df = spark.read.table(table_cfg["source_table"]).dropDuplicates()

    # Empty source handling
    if df.rdd.isEmpty():
        logger.warning(
            f"Source table {table_cfg['source_table']} is empty. "
            f"No SCD processing will be performed for target table {table_cfg['target_table']}."
        )
        return None

    df = preprocess_source(table_cfg, df)
    logger.info("Source preprocessing completed")
    return df


def transform_source(df, table_cfg):
    """
    Applies attribute transformations 
    """
    print("table_cfg",table_cfg)
    df = apply_transformations(df, table_cfg["attribute_logic"])
    df = df.withColumn("BATCH_ID_transformed",lit(table_cfg["batch_id"]))

    logger.info("Attribute transformations applied ")
    return df



def add_hash_column(df, table_cfg):
    """
    Adds HASH_VALUE_transformed using transformed business columns 
    """
    hash_cols = [
        coalesce(col(f"{c}_transformed").cast("string"), lit(""))
        for c in table_cfg.get("hashCol", [])
    ]
 
    if not hash_cols:
        logger.warning("No hash columns defined — HASH_VALUE_transformed set to NULL")
        return df.withColumn("HASH_VALUE_transformed", lit(None))
 
    return df.withColumn(
        "HASH_VALUE_transformed",
        sha2(concat_ws("~", *hash_cols), 256)
    )



def add_surrogate_key(df, table_cfg):
    """
    Adds a surrogate key column if configured.
    """
    if not table_cfg.get("surrogate_key"):
        return df

    surrogate_col = table_cfg["surrogate_key"]["column"]
    logger.info(f"Generating surrogate key: {surrogate_col}")

    return df.withColumn(
        surrogate_col,
        (abs(rand()) * 9223372036854775807).cast("bigint")
    )


# ---------------------------------------------------------
# Merge helpers
# ---------------------------------------------------------

def build_merge_condition(table_cfg):
    """
    Builds the merge condition string from the template and config.
    """
    merge_condition = table_cfg["merge_condition_template"].format(**table_cfg)
    logger.info(f"Merge condition: {merge_condition}")
    return merge_condition




def apply_scd1(target_dt, df, table_cfg, merge_condition):
    """
    Applies SCD Type 1 logic:
    - Overwrite changed records
    - Mark deleted records in-place
    """
    logger.info("Running SCD TYPE 1 logic")
 
    # Condition: change OR delete
    scd1_condition = f"""
        ({table_cfg["change_condition"]})
    """
 
    target_dt.alias("target") \
        .merge(df.alias("source_transformed"), merge_condition) \
        .whenMatchedUpdate(
            condition=scd1_condition,
            set={
                # Dynamically build update mapping for all configured business attributes.
                # For each attribute, update target column = corresponding transformed source column.
                # Excludes valid_from column as this is not applicable for SCD type 1
                **{
                    attr: f"source_transformed.{attr}_transformed"
                    for attr in table_cfg["attributes"]
                    if attr not in [table_cfg["valid_from_col"], "BATCH_ID"]
                },

                # Update audit column
                table_cfg["updated_col"]: current_timestamp()
            }
        ) \
        .execute()
 
    logger.info(f"SCD1 applied for table {table_cfg['target_table']}")


def apply_scd2(target_dt, df, table_cfg, merge_condition):
    """
    Applies SCD Type 2 logic:
    - Step 1: expire old records
    - Step 2: insert new/changed records
    """
    logger.info("Running SCD TYPE 2 logic")

    # Step 1: Expire old records
    target_dt.alias("target") \
        .merge(df.alias("source_transformed"), merge_condition) \
        .whenMatchedUpdate(
            condition=table_cfg["change_condition"],
            set={
                table_cfg["valid_to_col"]:
                    f"source_transformed.{table_cfg['valid_from_col']}_transformed"
            }
        ).execute()

    logger.info("Expired old records as part of SCD Type 2")

    # Step 2: Insert new/changed records done by the apply common inserts function





def apply_common_inserts(target_dt, source_transformed, table_cfg):
    """
    Inserts new rows for both SCD1 and SCD2 when no match is found.
    """
    # Build insert values
    insert_values = {}

    # Add surrogate key if defined
    if table_cfg.get("surrogate_key"):
        insert_values[table_cfg["surrogate_key"]["column"]] = \
            f"source_transformed.{table_cfg['surrogate_key']['column']}"

    # Add all transformed attributes
    for attr in table_cfg["attributes"]:
        insert_values[attr] = f"source_transformed.{attr}_transformed"
    # Insert filter from config 
    insert_condition = table_cfg.get("insert_condition")
    insert_merge_condition = table_cfg["insert_merge_condition_template"].format(**table_cfg)


    # Execute insert
    target_dt.alias("target") \
        .merge(source_transformed.alias("source_transformed"), insert_merge_condition) \
        .whenNotMatchedInsert(condition=insert_condition,values=insert_values) \
        .execute()

    print(f"Common insert applied for table {table_cfg['target_table']}")


# ---------------------------------------------------------
# Main Orchestrator
# ---------------------------------------------------------

def run_scd(table_cfg):
    """
    Orchestrates the full SCD process (Type 1 or 2) for a given table configuration.
    Handles:
    - config validation
    - empty source handling
    - source transforms
    - FK validation
    - SCD1 / SCD2 merge logic
    """
    try:
        # Validate table configuration first
        validate_table_cfg(table_cfg)

        logger.info(f"Starting SCD process for table: {table_cfg['target_table']}")

        # Load and preprocess source
        df = load_and_preprocess_source(table_cfg)

        # Handle empty source safely
        if df is None:
            logger.info(
                f"Skipping SCD process for {table_cfg['target_table']} "
                f"because source table {table_cfg['source_table']} is empty"
            )
            return

        # Transform and enrich source
        df = transform_source(df, table_cfg)
        df = add_hash_column(df, table_cfg)
        df = add_surrogate_key(df, table_cfg)

        # Foreign key validation
        df = validate_foreign_keys(df, table_cfg)
        

        # Target table and merge condition
        target_dt = DeltaTable.forName(spark, table_cfg["target_table"])
        merge_condition = build_merge_condition(table_cfg)

        # SCD type selection
        scd_type = table_cfg.get("scd_type", "TYPE_2").upper()
        print(scd_type)

        if scd_type == "TYPE_1":
            apply_scd1(target_dt, df, table_cfg, merge_condition)
        else:
            apply_scd2(target_dt, df, table_cfg, merge_condition)
        df_for_insert = df
        
        
        # Common insert step for both SCD1 and SCD2 apply_common_inserts if no match
        apply_common_inserts(target_dt, df_for_insert, table_cfg)

        logger.info(f"SCD process completed successfully for {table_cfg['target_table']}")
        
        #### SOURCE AND TARGET COUNT
        history = target_dt.history(1).select(
            "operationMetrics.numTargetRowsInserted",
            "operationMetrics.numTargetRowsUpdated",
            "operationMetrics.numTargetRowsDeleted"
        ).collect()[0]
        source_count = df.count()
        insert_count = int(history["numTargetRowsInserted"] or 0)
        update_count = int(history["numTargetRowsUpdated"] or 0)
        delete_count = int(history["numTargetRowsDeleted"] or 0)
        
        
        audit_schema = StructType([
            StructField("BATCH_ID", StringType(), False),
            StructField("SOURCE_TABLE", StringType(), False),
            StructField("TARGET_TABLE", StringType(), False),
            StructField("CREATED_DATE", TimestampType(), False),
            StructField("SOURCE_RECORDS", LongType(), False),
            StructField("INSERT_RECORDS", LongType(), False),
            StructField("UPDATE_RECORDS", LongType(), False),
            StructField("DELETE_RECORDS", LongType(), False)
        ])

        audit_df = spark.createDataFrame([
            (
            str(table_cfg["batch_id"]),
            table_cfg["source_table"],
            table_cfg["target_table"],
            datetime.now(),
            int(source_count),     
            int(insert_count),
            int(update_count),
            int(delete_count)
            )
        ], audit_schema)

        audit_df.write.format("delta") \
            .mode("append") \
            .saveAsTable(audit_tbl)



    except Exception as ex:
        logger.error(f"SCD process failed for table {table_cfg.get('target_table', 'UNKNOWN')}: {ex}")
        raise Exception (f"SCD process failed for table {table_cfg.get('target_table', 'UNKNOWN')}: {ex}")

from delta.tables import DeltaTable
import uuid
from pyspark.sql.functions import current_date, lit, expr,sha2,concat_ws,col,rand,abs,when,length,trim,current_timestamp,coalesce
from pyspark.sql import functions as F


# Tables smaller than this size (in bytes) will be broadcast automatically
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10 MB

for cfg in tables_config:
    run_scd(cfg)
mssparkutils.notebook.exit("success")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
