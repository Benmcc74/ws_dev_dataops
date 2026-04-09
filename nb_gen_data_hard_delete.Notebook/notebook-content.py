# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Sample parameters for understanding
# target_table = "LHDEVBRONZE.bronze_staging.customer_bronze_snapshot"
# target_id_col = "ID"
# batch_id = "test run"
# source_table = "LHDEVBRONZE.bronze_staging.customer_bronze_snapshot"
# delete_attribute = "DELETEDINCORE"
# delete_attribute_value = "Y"
# source_id_col = "ID"
# rule_validation_results_table_name = "LHDEVBRONZE.audit.ops_validation_results"
# schema_validation_results_table_name = "LHDEVBRONZE.audit.ops_schema_validation_results"
# record_identifier_column = "RECORD_IDENTIFIER"
# record_identifier_value_column = "RECORD_IDENTIFIER_VALUE"
# validation_source_name_column = 'SOURCE_NAME'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql.functions import col, lit, current_timestamp

def create_temp_delete_events(
    spark,
    source_table: str,          
    delete_attribute: str,     
    delete_attribute_value: str,
    source_id_col: str
):
    try:
        df = (
            spark.read.table(source_table)
                .filter(col(delete_attribute) == delete_attribute_value)
                .select(col(source_id_col).alias(source_id_col))
                .distinct()
        )

        delete_count = df.count()

        if delete_count == 0:
            print("No events to delete. Exiting pipeline.")
            mssparkutils.notebook.exit("No events to delete. Exiting pipeline.")

        print(f"{delete_count} delete events found.")
        return df

    except Exception as e:
        print(f"ERROR in create_temp_delete_events: {str(e)}")
        raise


def run_generic_delete(
    spark,
    target_table: str,
    target_id_col: str,
    delete_ids_df,    
    batch_id: str,
    source_table: str,
    source_id_col: str,
    rule_validation_results_table_name: str = None,
    schema_validation_results_table_name: str = None,
    record_identifier_column: str = None,
    record_identifier_value_column: str = None,
    validation_source_name_column: str = None
):
    try:
        # 1. Load target IDs
        tgt_df = spark.table(target_table).select(target_id_col).distinct()

        # 2. Identify only IDs that actually exist in target
        matched_ids_df = (
            delete_ids_df.join(
                tgt_df,
                delete_ids_df[source_id_col] == tgt_df[target_id_col],
                "inner"
            )
            .select(
                delete_ids_df[source_id_col].alias(source_id_col),
                delete_ids_df[source_id_col].alias("ATTRIBUTE_VALUE")
            )
            .distinct()
        )

        matched_count = matched_ids_df.count()
        print(f"{matched_count} records match in main table {target_table}")

        # ----------------------------------------------------------------------
        # VALIDATION TABLE DELETE BLOCK 
        # ----------------------------------------------------------------------
        validation_tables = [
            t for t in [
                rule_validation_results_table_name,
                schema_validation_results_table_name
            ] if t
        ]


        if (
             validation_tables
             and record_identifier_column
             and record_identifier_value_column
             and validation_source_name_column
             ):

            delete_ids_df.createOrReplaceTempView("delete_ids_view")

            for vtable in validation_tables:
                print(f"Processing validation table: {vtable}")
                ## Extracts the table name without lakehouse name
                deleted_rows_df = spark.sql(f"""
                    SELECT tgt.{record_identifier_value_column} AS deleted_id
                    FROM {vtable} tgt
                    JOIN (
                        SELECT
                            {source_id_col} AS src_id,
                            substring(
                                '{source_table}',
                                instr('{source_table}', '.') + 1,
                                length('{source_table}')
                            ) AS src_target_table 
                        FROM delete_ids_view
                    ) src
                        ON tgt.{record_identifier_column} = '{source_id_col}'
                       AND tgt.{record_identifier_value_column} = src.src_id
                       AND tgt.{validation_source_name_column} = src.src_target_table
                """)

                delete_count = deleted_rows_df.count()
                print(f"{delete_count} rows will be deleted from {vtable}")

                # ---------------------------
                # VALIDATION DELETE ONLY IF delete_count > 0
                # ---------------------------
                if delete_count > 0:

                    audit_val_df = (
                        deleted_rows_df
                            .withColumn("ATTRIBUTE_NAME", lit(source_id_col))
                            .withColumn("ATTRIBUTE_VALUE", col("deleted_id"))
                            .withColumn("EVENT_TYPE", lit("DELETE"))
                            .withColumn("SOURCE_TABLE", lit(source_table))
                            .withColumn("TARGET_TABLE", lit(vtable))
                            .withColumn("DELETED_DATE", current_timestamp())
                            .withColumn("BATCH_ID", lit(batch_id))
                            .drop("deleted_id")
                    )

                    audit_val_df = spark.createDataFrame(audit_val_df.collect(), audit_val_df.schema)

                    merge_sql_val = f"""
                    MERGE INTO {vtable} AS tgt
                    USING (
                        SELECT
                            {source_id_col} AS src_id,
                            substring(
                                '{source_table}',
                                instr('{source_table}', '.') + 1,
                                length('{source_table}')
                            ) AS src_target_table
                        FROM delete_ids_view
                    ) AS src
                        ON tgt.{record_identifier_column} = '{source_id_col}'
                       AND tgt.{record_identifier_value_column} = src.src_id
                       AND tgt.{validation_source_name_column} = src.src_target_table
                    WHEN MATCHED THEN DELETE
                    """

                    spark.sql(merge_sql_val)
                    print(f"Deleted from {vtable}")

                    audit_val_df.write.format("delta").mode("append").saveAsTable("audit.ops_delete_log")

                    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
                    spark.sql(f"VACUUM {vtable} RETAIN 0 HOURS")
                    print(f"Vacuum completed for {vtable}")

                else:
                    print(f"No rows to delete from {vtable}. Skipping delete, audit, and vacuum.")

        # ----------------------------------------------------------------------
        # MAIN TABLE DELETE BLOCK 
        # ----------------------------------------------------------------------

        if matched_count > 0:   #  MAIN DELETE ONLY IF matched_count > 0
            print(f"Deleting {matched_count} rows from main table {target_table}")

            audit_df = (
                matched_ids_df
                    .withColumn("ATTRIBUTE_NAME", lit(target_id_col))
                    .withColumn("EVENT_TYPE", lit("DELETE"))
                    .withColumn("SOURCE_TABLE", lit(source_table))
                    .withColumn("TARGET_TABLE", lit(target_table))
                    .withColumn("DELETED_DATE", current_timestamp())
                    .withColumn("BATCH_ID", lit(batch_id))
            ).drop(source_id_col)

            audit_df = spark.createDataFrame(audit_df.collect(), audit_df.schema)

            matched_ids_df.createOrReplaceTempView("matched_ids_view")


            merge_sql = f"""
            MERGE INTO {target_table} AS T
            USING matched_ids_view AS D
            ON T.{target_id_col} = D.{source_id_col}
            WHEN MATCHED THEN DELETE
            """

            spark.sql(merge_sql)
            print("Hard delete executed on", target_table)

            audit_df.write.format("delta").mode("append").saveAsTable("audit.ops_delete_log")
            print("Audit log written")

            spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            spark.sql(f"VACUUM {target_table} RETAIN 0 HOURS")
            print("Vacuum completed for main table")

        else:
            print("No matching rows in main table. Skipping main delete and vacuum.")

    except Exception as e:
        print(f"ERROR during delete pipeline: {str(e)}")
        raise

    finally:
        for view in ["matched_ids_view", "delete_ids_view"]:
            try:
                spark.catalog.dropTempView(view)
            except:
                pass
                
# 1. Get delete IDs as a DataFrame
delete_ids_df = create_temp_delete_events(
    spark,
    source_table,          
    delete_attribute,     
    delete_attribute_value,
    source_id_col
)


# 2. Run delete using the DataFrame

run_generic_delete(
    spark,
    target_table,
    target_id_col,
    delete_ids_df,
    batch_id,
    source_table,
    source_id_col,
    globals().get("rule_validation_results_table_name"),
    globals().get("schema_validation_results_table_name"),
    globals().get("record_identifier_column"),
    globals().get("record_identifier_value_column"),
    globals().get("validation_source_name_column")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
