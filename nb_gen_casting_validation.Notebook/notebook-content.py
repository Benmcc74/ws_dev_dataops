# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql.functions import col, trim, when, lit, current_timestamp, concat_ws
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)
from datetime import datetime
from collections import defaultdict
from functools import reduce
import logging

# ---------------------------------------------------------
# Logger
# ---------------------------------------------------------
logger = logging.getLogger("DQ-Schema-Validation")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.propagate = False

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# ---------------------------------------------------------
# Audit schema
# ---------------------------------------------------------
RESULT_SCHEMA = StructType([
    StructField("BATCH_ID", StringType(), True),
    StructField("SOURCE_TYPE", StringType(), True),
    StructField("SOURCE_NAME", StringType(), False),
    StructField("VALIDATION_STAGE", StringType(), False),
    StructField("ACTUAL_COLUMN_NAME", StringType(), True),
    StructField("EXPECTED_DATA_TYPE", StringType(), True),
    StructField("ACTUAL_DATA_TYPE", StringType(), True),
    StructField("ATTRIBUTE_VALUE", StringType(), True),
    StructField("RECORD_IDENTIFIER", StringType(), True),
    StructField("RECORD_IDENTIFIER_VALUE", StringType(), True),
    StructField("VALIDATION_STATUS", StringType(), False),
    StructField("VALIDATION_MESSAGE", StringType(), True),
    StructField("IS_FIXED", StringType(), True),
    StructField("FIXED_DATE", TimestampType(), True),
    StructField("CREATED_DATE", TimestampType(), False)
])

# ---------------------------------------------------------
# Load rules
# ---------------------------------------------------------
def load_data_type_rules(spark, interface_id):
    return (
        spark.table("control.cfg_expected_schema")
             .filter(col("INTERFACE_ID") == interface_id)
             .filter(col("DATA_TYPE_VALIDATION").isNotNull())
             .filter(trim(col("DATA_TYPE_VALIDATION")) != "")
             .select("SOURCE_TABLE_NAME", "COLUMN_NAME", "DATA_TYPE_VALIDATION", "RECORD_IDENTIFIER")
             .distinct()
    )

# ---------------------------------------------------------
# Group rules
# ---------------------------------------------------------
def group_schema_rules(rules_df):
    table_rules = defaultdict(list)
    for row in rules_df.collect():
        table_rules[row.SOURCE_TABLE_NAME].append(
            (row.COLUMN_NAME, row.DATA_TYPE_VALIDATION, row.RECORD_IDENTIFIER)
        )
    return table_rules

# ---------------------------------------------------------
# Validate column type 
# ---------------------------------------------------------
def validate_column_type(df, col_name, expected_type, record_identifier):
    actual_type = df.schema[col_name].dataType.simpleString()

    casted_df = df.withColumn("cast_test", col(col_name).cast(expected_type))

    invalid_df = (
        casted_df.filter(
            col("cast_test").isNull() &
            col(col_name).isNotNull() &
            (trim(col(col_name)) != "")
        )
        .select(
            col(col_name).alias("invalid_value"),
            col(record_identifier).alias("record_identifier_col")
        )
    )

    if not invalid_df.head(1):
        return "PASS", "Column successfully cast to expected type", actual_type, None

    return "FAIL", f"{col_name} cannot be cast to {expected_type}", actual_type, invalid_df

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
def validate_data_types(interface_id, batch_id):
    spark = SparkSession.builder.appName("DQ-Schema-Validation").getOrCreate()

    rules_df = load_data_type_rules(spark, interface_id)
    table_rules = group_schema_rules(rules_df)

    audit_df = spark.table("audit.ops_schema_validation_results") \
                    .filter(col("VALIDATION_STAGE") == "DATA_TYPE_CHECK")

    any_fail = False

    for TABLE_NAME, rules in table_rules.items():
        try:
            df = spark.table(TABLE_NAME)
        except Exception as ex:
            logger.error(f"Unable to read table '{TABLE_NAME}': {ex}")
            continue

        logger.info(f"Processing table: {TABLE_NAME}")

        fail_audit_dfs = []
        rectified_audit_dfs = []
        fail_ids_dfs = []

        for COLUMN_NAME, EXPECTED_TYPE, RECORD_IDENTIFIER in rules:

            if COLUMN_NAME not in df.columns:
                logger.error(f"Column '{COLUMN_NAME}' missing in table '{TABLE_NAME}'. Skipping.")
                continue

            status, message, actual_type, invalid_df = validate_column_type(
                df, COLUMN_NAME, EXPECTED_TYPE, RECORD_IDENTIFIER
            )

            # previous FAILs
            prev_fail_df = (
                audit_df.filter(
                    (col("SOURCE_NAME") == TABLE_NAME) &
                    (col("ACTUAL_COLUMN_NAME") == COLUMN_NAME) &
                    (col("RECORD_IDENTIFIER") == RECORD_IDENTIFIER) &
                    (col("VALIDATION_STATUS") == "FAIL") &
                    (col("IS_FIXED") == "N")
                )
                .select("RECORD_IDENTIFIER_VALUE")
            )

            # current FAILs
            if invalid_df is not None and invalid_df.head(1):
                current_fail_df = (
                    invalid_df
                    .select(col("record_identifier_col").alias("RECORD_IDENTIFIER_VALUE"))
                    .distinct()
                )
                fail_ids_dfs.append(current_fail_df)

                fail_audit_df = (
                    invalid_df
                    .withColumn("BATCH_ID", lit(batch_id))
                    .withColumn("SOURCE_TYPE", lit("Table"))
                    .withColumn("SOURCE_NAME", lit(TABLE_NAME))
                    .withColumn("VALIDATION_STAGE", lit("DATA_TYPE_CHECK"))
                    .withColumn("ACTUAL_COLUMN_NAME", lit(COLUMN_NAME))
                    .withColumn("EXPECTED_DATA_TYPE", lit(EXPECTED_TYPE))
                    .withColumn("ACTUAL_DATA_TYPE", lit(actual_type))
                    .withColumn("ATTRIBUTE_VALUE", col("invalid_value").cast("string"))
                    .withColumn("RECORD_IDENTIFIER", lit(RECORD_IDENTIFIER))
                    .withColumn("RECORD_IDENTIFIER_VALUE", col("record_identifier_col").cast("string"))
                    .withColumn("VALIDATION_STATUS", lit("FAIL"))
                    .withColumn("VALIDATION_MESSAGE", lit(message))
                    .withColumn("IS_FIXED", lit("N"))
                    .withColumn("FIXED_DATE", lit(None).cast(TimestampType()))
                    .withColumn("CREATED_DATE", current_timestamp())
                    .select([f.name for f in RESULT_SCHEMA.fields])
                )
                fail_audit_dfs.append(fail_audit_df)
                any_fail = True
            else:
                current_fail_df = spark.createDataFrame(
                    [], "RECORD_IDENTIFIER_VALUE string"
                )

            # current PASS
            current_pass_df = (
                df.select(col(RECORD_IDENTIFIER).alias("RECORD_IDENTIFIER_VALUE"))
                  .distinct()
                  .join(current_fail_df, "RECORD_IDENTIFIER_VALUE", "left_anti")
            )

            # rectified = prev_fail ∩ current_pass
            rectified_ids_df = current_pass_df.join(
                prev_fail_df, "RECORD_IDENTIFIER_VALUE", "inner"
            )

            rectified_ids_df = rectified_ids_df.join(
             df.select(
             col(RECORD_IDENTIFIER).alias("RECORD_IDENTIFIER_VALUE"),
             col(COLUMN_NAME).cast("string").alias("ATTRIBUTE_VALUE")
                ),
             "RECORD_IDENTIFIER_VALUE",
             "left"
             )

            if rectified_ids_df.head(1):
                rectified_audit_df = (
                    rectified_ids_df
                    .withColumn("BATCH_ID", lit(batch_id))
                    .withColumn("SOURCE_TYPE", lit("Table"))
                    .withColumn("SOURCE_NAME", lit(TABLE_NAME))
                    .withColumn("VALIDATION_STAGE", lit("DATA_TYPE_CHECK"))
                    .withColumn("ACTUAL_COLUMN_NAME", lit(COLUMN_NAME))
                    .withColumn("EXPECTED_DATA_TYPE", lit(EXPECTED_TYPE))
                    .withColumn("ACTUAL_DATA_TYPE", lit(actual_type))
                    .withColumn("ATTRIBUTE_VALUE", col("ATTRIBUTE_VALUE").cast("string"))
                    .withColumn("RECORD_IDENTIFIER", lit(RECORD_IDENTIFIER))
                    .withColumn("RECORD_IDENTIFIER_VALUE", col("RECORD_IDENTIFIER_VALUE").cast("string"))
                    .withColumn("VALIDATION_STATUS", lit("PASS"))
                    .withColumn("VALIDATION_MESSAGE", lit("Value rectified"))
                    .withColumn("IS_FIXED", lit("Y"))
                    .withColumn("FIXED_DATE", current_timestamp())
                    .withColumn("CREATED_DATE", current_timestamp())
                    .select([f.name for f in RESULT_SCHEMA.fields])
                )
                rectified_audit_dfs.append(rectified_audit_df)

        # write all FAIL + RECTIFIED rows for this table
        combined_audit_dfs = fail_audit_dfs + rectified_audit_dfs
        if combined_audit_dfs:
            final_audit_df = reduce(lambda a, b: a.unionByName(b), combined_audit_dfs)
            final_audit_df.write.format("delta").mode("append").saveAsTable(
                "audit.ops_schema_validation_results"
            )

        # single MERGE for rectified rows per table
        if rectified_audit_dfs:
            all_rectified_df = reduce(lambda a, b: a.unionByName(b), rectified_audit_dfs)
            all_rectified_df.createOrReplaceTempView("rectified_tmp")

            spark.sql("""
                MERGE INTO audit.ops_schema_validation_results AS tgt
                USING rectified_tmp AS src
                ON  tgt.SOURCE_NAME                 = src.SOURCE_NAME
                AND tgt.ACTUAL_COLUMN_NAME          = src.ACTUAL_COLUMN_NAME
                AND tgt.RECORD_IDENTIFIER = src.RECORD_IDENTIFIER
                AND tgt.RECORD_IDENTIFIER_VALUE     = src.RECORD_IDENTIFIER_VALUE
                AND tgt.VALIDATION_STAGE            = src.VALIDATION_STAGE
                AND tgt.VALIDATION_STATUS           = 'FAIL'
                AND tgt.IS_FIXED                    = 'N'
                WHEN MATCHED THEN
                  UPDATE SET
                    tgt.IS_FIXED   = 'Y',
                    tgt.FIXED_DATE = current_timestamp()
            """)

        # CASTING_VALIDATION_RESULT per table
        if fail_ids_dfs:
            invalid_ids_df = reduce(lambda a, b: a.unionByName(b), fail_ids_dfs).distinct()

            df_flag = (
                df.join(
                    invalid_ids_df,
                    df[RECORD_IDENTIFIER] == invalid_ids_df["RECORD_IDENTIFIER_VALUE"],
                    "left"
                )
                .withColumn(
                    "CASTING_VALIDATION_RESULT",
                    when(col("RECORD_IDENTIFIER_VALUE").isNotNull(), lit("FAIL"))
                    .when(col("CASTING_VALIDATION_RESULT") == "FAIL", lit("FAIL"))
                    .otherwise(lit("PASS"))
                )
                .select(RECORD_IDENTIFIER, "CASTING_VALIDATION_RESULT")
            )
        else:
            df_flag = (
                df.withColumn(
                    "CASTING_VALIDATION_RESULT",
                    when(col("CASTING_VALIDATION_RESULT") == "FAIL", lit("FAIL"))
                    .otherwise(lit("PASS"))
                )
                .select(RECORD_IDENTIFIER, "CASTING_VALIDATION_RESULT")
            )

        df_flag = df_flag.dropDuplicates([RECORD_IDENTIFIER])

        df_flag.createOrReplaceTempView("updated_casting_tmp")

        spark.sql(f"""
            MERGE INTO {TABLE_NAME} AS T
            USING updated_casting_tmp AS S
            ON T.{RECORD_IDENTIFIER} = S.{RECORD_IDENTIFIER}
            WHEN MATCHED THEN UPDATE SET
              T.CASTING_VALIDATION_RESULT = S.CASTING_VALIDATION_RESULT
        """)

        logger.info(f"CASTING_VALIDATION_RESULT updated for table {TABLE_NAME}")

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
            df_flag.createOrReplaceTempView("updated_casting_hist_tmp")
            spark.sql(f"""
                MERGE INTO {HIST_TBL} AS T
                USING updated_casting_hist_tmp AS S
                ON T.{RECORD_IDENTIFIER} = S.{RECORD_IDENTIFIER}
                AND T.BATCH_ID = '{batch_id}'
                WHEN MATCHED THEN UPDATE SET
                    T.CASTING_VALIDATION_RESULT = S.CASTING_VALIDATION_RESULT
            """)
            logger.info(f"CASTING_VALIDATION_RESULT updated for table {HIST_TBL}")

    if any_fail:
        mssparkutils.notebook.exit("FAIL")
    else:
        mssparkutils.notebook.exit("PASS")

# ---------------------------------------------------------
# Run
# ---------------------------------------------------------
validate_data_types(interface_id, batch_id)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
