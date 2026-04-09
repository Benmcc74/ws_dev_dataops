# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run nb_gen_rules_validation_functions


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    when,
    concat_ws,
    trim,
    broadcast,
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from collections import defaultdict
from functools import reduce

# ----------------------------------------------------------
# Logging Setup
# ----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("DQ-Validation")

# ---------------------------------------------------------
# Audit Schema
# ---------------------------------------------------------
def audit_schema():
    return StructType([
        StructField("BATCH_ID", StringType(), True),
        StructField("RULE_ASSIGNMENT_ID", IntegerType(), True),
        StructField("INTERFACE_ID", StringType(), True),
        StructField("RULE_TYPE", StringType(), True),
        StructField("SOURCE_NAME", StringType(), True),
        StructField("COLUMN_NAME", StringType(), True),
        StructField("RECORD_IDENTIFIER", StringType(), True),
        StructField("RECORD_IDENTIFIER_VALUE", StringType(), True),
        StructField("VALIDATION_STATUS", StringType(), True),
        StructField("IS_FIXED", StringType(), True),
        StructField("FIXED_DATE", TimestampType(), True),
        StructField("CREATED_DATE", TimestampType(), True)
    ])

# ---------------------------------------------------------
# Load Rules
# ---------------------------------------------------------
def load_rules(spark, interface_id):
    logger.info("Loading validation rules")
    return (
        spark.table("control.cfg_validation_rules_assignment")
        .filter(
            (col("INTERFACE_ID") == interface_id) &
            (col("IS_ACTIVE") == "Y")
        )
        .select(
            "RULE_ASSIGNMENT_ID",
            "INTERFACE_ID",
            "RULE_TYPE",
            "TABLE_NAME",
            "TARGET_COLUMN",
            "RECORD_IDENTIFIER",
            "CONDITION_COLUMN"
        )
    )

# ---------------------------------------------------------
# Group Rules by Table
# ---------------------------------------------------------
def group_rules(rules_df):
    table_rules = defaultdict(list)
    for row in rules_df.collect():
        table_rules[row.TABLE_NAME].append(
            (
                row.TARGET_COLUMN,
                row.RULE_ASSIGNMENT_ID,
                row.INTERFACE_ID,
                row.RULE_TYPE,
                row.RECORD_IDENTIFIER,
                row.CONDITION_COLUMN
            )
        )
    return table_rules

# ---------------------------------------------------------
# Read Table
# ---------------------------------------------------------
def read_table(spark, table_name):
    try:
        df = spark.table(table_name)
        if not df.head(1):
            logger.warning(f"{table_name} empty")
            return None
        return df
    except Exception as e:
        logger.error(f"Error reading {table_name}: {e}")
        return None

# ---------------------------------------------------------
# Write Audit
# ---------------------------------------------------------
def write_audit(df):
    if df is None or not df.head(1):
        return
    df.write.mode("append").saveAsTable("audit.ops_validation_results")

# ---------------------------------------------------------
# MAIN VALIDATION
# ---------------------------------------------------------
def run_validation(interface_id, function_map):
    spark = SparkSession.builder.appName("DQ-Validation").getOrCreate()

    rules_df = load_rules(spark, interface_id)
    rules_df = broadcast(rules_df)
    table_rules = group_rules(rules_df)

    audit_df = (
        spark.table("audit.ops_validation_results")
        .filter(col("INTERFACE_ID") == interface_id)
    )

    for TABLE_NAME, rules in table_rules.items():
        df = read_table(spark, TABLE_NAME)
        if df is None:
            continue

        df = df.cache()
        df.count()

        audit_dfs = []
        rectified_dfs = []
        fail_ids_df = None

        # This loop runs by rule id, table, column level
        for (
            TARGET_COLUMN,
            RULE_ASSIGNMENT_ID,
            INTERFACE_ID,
            RULE_TYPE,
            RECORD_IDENTIFIER,
            CONDITION_COLUMN
        ) in rules:

            if RULE_TYPE not in function_map:
                continue

            record_id_col = RECORD_IDENTIFIER

            # This runs the rule validation and produces the status_df with the failed ids
            try:
                if RULE_TYPE == "Conditional Null Check":
                    status_df = function_map[RULE_TYPE](
                        df, TARGET_COLUMN, RECORD_IDENTIFIER, CONDITION_COLUMN
                    )
                elif RULE_TYPE == "Valid Date for Y Flag":
                   status_df = function_map[RULE_TYPE](
                   df, TARGET_COLUMN, RECORD_IDENTIFIER, CONDITION_COLUMN
                   )
                else:
                    status_df = function_map[RULE_TYPE](
                        df, TARGET_COLUMN, RECORD_IDENTIFIER
                    )
            except Exception as e:
                logger.error(e)
                continue

            # This produces just the failed records from audit results table based on filter conditions
            prev_fail_df = (
                audit_df
                .filter(
                    (col("RULE_ASSIGNMENT_ID") == RULE_ASSIGNMENT_ID) &
                    (col("INTERFACE_ID") == INTERFACE_ID) &
                    (col("RULE_TYPE") == RULE_TYPE) &
                    (col("SOURCE_NAME") == TABLE_NAME) &
                    (col("COLUMN_NAME") == TARGET_COLUMN) &
                    (col("RECORD_IDENTIFIER") == RECORD_IDENTIFIER) &
                    (col("VALIDATION_STATUS") == "FAIL") &
                    (col("IS_FIXED") == "N")
                )
                .select("RECORD_IDENTIFIER_VALUE")
            )

            # This builds current fails only
            if status_df is not None and status_df.head(1):
                current_fail_df = (
                    status_df
                    .select(col("record_identifier_col").alias("RECORD_IDENTIFIER_VALUE"))
                    .distinct()
                )

                # Accumulate invalid ids as DataFrame (no collect inside loop)
                if fail_ids_df is None:
                    fail_ids_df = current_fail_df.select("RECORD_IDENTIFIER_VALUE")
                else:
                    fail_ids_df = fail_ids_df.union(
                        current_fail_df.select("RECORD_IDENTIFIER_VALUE")
                    )
            else:
                current_fail_df = spark.createDataFrame(
                    [],
                    "RECORD_IDENTIFIER_VALUE string"
                )

            # This produces the current pass
            current_pass_df = (
                df.select(col(record_id_col).alias("RECORD_IDENTIFIER_VALUE"))
                .distinct()
                .join(current_fail_df, "RECORD_IDENTIFIER_VALUE", "left_anti")
            )

            # Prepares df to insert current fails into the result table
            fail_audit_df = (
                current_fail_df
                .withColumn("BATCH_ID", lit(batch_id))
                .withColumn("RULE_ASSIGNMENT_ID", lit(RULE_ASSIGNMENT_ID))
                .withColumn("INTERFACE_ID", lit(INTERFACE_ID))
                .withColumn("RULE_TYPE", lit(RULE_TYPE))
                .withColumn("SOURCE_NAME", lit(TABLE_NAME))
                .withColumn("COLUMN_NAME", lit(TARGET_COLUMN))
                .withColumn("RECORD_IDENTIFIER", lit(RECORD_IDENTIFIER))
                .withColumn("VALIDATION_STATUS", lit("FAIL"))
                .withColumn("IS_FIXED", lit("N"))
                .withColumn("FIXED_DATE", lit(None).cast(TimestampType()))
                .withColumn("CREATED_DATE", current_timestamp())
            )

            # Prepares df only for pass which failed previously and is fixed now to enter into the result table
            rectified_df = (
                current_pass_df
                .join(prev_fail_df, "RECORD_IDENTIFIER_VALUE", "inner")
                .withColumn("BATCH_ID", lit(batch_id))
                .withColumn("RULE_ASSIGNMENT_ID", lit(RULE_ASSIGNMENT_ID))
                .withColumn("INTERFACE_ID", lit(INTERFACE_ID))
                .withColumn("RULE_TYPE", lit(RULE_TYPE))
                .withColumn("SOURCE_NAME", lit(TABLE_NAME))
                .withColumn("COLUMN_NAME", lit(TARGET_COLUMN))
                .withColumn("RECORD_IDENTIFIER", lit(RECORD_IDENTIFIER))
                .withColumn("VALIDATION_STATUS", lit("PASS"))
                .withColumn("IS_FIXED", lit("Y"))
                .withColumn("FIXED_DATE", current_timestamp())
                .withColumn("CREATED_DATE", current_timestamp())
            )

            final_audit_df = fail_audit_df.unionByName(rectified_df)
            audit_dfs.append(final_audit_df)
            rectified_dfs.append(rectified_df)

        # Write all audit rows for this table in one go
        if audit_dfs:
            all_audit_df = reduce(lambda a, b: a.unionByName(b), audit_dfs)
            write_audit(all_audit_df)

        # Single MERGE for rectified rows per table
        if rectified_dfs:
            all_rectified_df = reduce(lambda a, b: a.unionByName(b), rectified_dfs)
            all_rectified_df.createOrReplaceTempView("rectified_tmp")

            spark.sql("""
                MERGE INTO audit.ops_validation_results tgt
                USING rectified_tmp src
                ON tgt.RULE_ASSIGNMENT_ID = src.RULE_ASSIGNMENT_ID
                AND tgt.INTERFACE_ID = src.INTERFACE_ID
                AND tgt.RULE_TYPE = src.RULE_TYPE
                AND tgt.SOURCE_NAME = src.SOURCE_NAME
                AND tgt.COLUMN_NAME = src.COLUMN_NAME
                AND tgt.RECORD_IDENTIFIER = src.RECORD_IDENTIFIER
                AND tgt.RECORD_IDENTIFIER_VALUE = src.RECORD_IDENTIFIER_VALUE
                AND tgt.VALIDATION_STATUS = 'FAIL'
                AND tgt.IS_FIXED = 'N'
                WHEN MATCHED THEN UPDATE SET
                tgt.IS_FIXED='Y',
                tgt.FIXED_DATE=current_timestamp()
            """)

        # Build invalid ids once per table 
        record_id_col = rules[0][4]

        if fail_ids_df is not None:
            invalid_ids_df = fail_ids_df.distinct()
            invalid_ids = [r[0] for r in invalid_ids_df.collect()]
        else:
            invalid_ids = []

        # Result is updated in the staging table irrespective of what rule it failed.
        # The result will still be updated as fail even if one rule fails
        if invalid_ids:
            invalid_df = spark.createDataFrame(
                [(x,) for x in invalid_ids],
                "record_identifier_col STRING"
            )
            df_flag = (
                df.join(
                    invalid_df,
                    df[record_id_col] == invalid_df["record_identifier_col"],
                    "left"
                )
                .withColumn(
                    "RULE_VALIDATION_RESULT",
                    when(col("record_identifier_col").isNotNull(), "FAIL").otherwise("PASS")
                )
                .drop("record_identifier_col")
            )
        else:
            df_flag = df.withColumn("RULE_VALIDATION_RESULT", lit("PASS"))

        df_flag = df_flag.dropDuplicates([record_id_col])

        df_flag.createOrReplaceTempView("rule_validation_tmp")
        spark.sql(f"""
            MERGE INTO {TABLE_NAME} AS T
            USING rule_validation_tmp AS S
            ON T.{record_id_col} = S.{record_id_col}
            WHEN MATCHED THEN UPDATE SET
            T.RULE_VALIDATION_RESULT = S.RULE_VALIDATION_RESULT
        """)

        logger.info(f"RULE_VALIDATION_RESULT updated for table {TABLE_NAME}")

        hist_df = (
            spark.table("control.cfg_bronze_staging")
            .filter(col("HISTORY_LOAD_INDICATOR") == "Y")
            .filter(concat_ws(".", col("TARGET_SCHEMA"), col("TARGET_TABLE_NAME")) == TABLE_NAME)
        )

        row = hist_df.select(
            concat_ws(".", col("HISTORY_TARGET_SCHEMA"), col("HISTORY_TARGET_TABLE_NAME")).alias("hist_tbl")
        ).head(1)

        if not row:
            logger.info(f"History table is not available for {TABLE_NAME}")
        else:
            HIST_TBL = row[0]["hist_tbl"]
            df_flag.createOrReplaceTempView("updated_history_tmp")
            spark.sql(f"""
                MERGE INTO {HIST_TBL} AS T
                USING updated_history_tmp AS S
                ON T.{record_id_col} = S.{record_id_col}
                AND T.BATCH_ID = '{batch_id}'
                WHEN MATCHED THEN UPDATE SET
                T.RULE_VALIDATION_RESULT = S.RULE_VALIDATION_RESULT
            """)

# ---------------------------------------------------------
# RULE FUNCTION MAP
# ---------------------------------------------------------
function_map = {
    "Duplicate Check": DuplicatesCheck,
    "Null Value Check": NullValueCheck,
    "Empty Value Check": EmptyValueCheck,
    "Spark Date Threshold Check": SparkDateThresholdCheck,
    "Conditional Null Check": ConditionalNullCheck,
    "Valid Date for Y Flag": DATERequiredWhenFlagY
}

# ---------------------------------------------------------
# EXECUTE
# ---------------------------------------------------------
run_validation(interface_id, function_map)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
