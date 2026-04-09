# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,current_timestamp,expr
from datetime import datetime
from notebookutils import mssparkutils
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from delta.tables import DeltaTable


audit_tbl = "audit.ops_record_count_log"
file_valid_tbl = "audit.ops_ingestion_file_audit"
# ---------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("BronzeStagingLoad")


# ---------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------
def normalize_delimiter(delimiter):
    """Normalize empty or null-like delimiter values."""
    if delimiter is None:
        return None
    if str(delimiter).strip().lower() in ("", "null"):
        return None
    return delimiter


def resolve_filename(source_object_name, today):
    """Resolve placeholders in the source object name."""
    filename = source_object_name.replace("hhmmss", "")
    print(filename)
    return (
        filename
        .replace("YYYY", today.strftime("%Y"))
        .replace("MM", today.strftime("%m"))
        .replace("DD", today.strftime("%d"))
    )


def resolve_path(path_template, today):
    """Replace date placeholders in folder paths."""
    return (
        path_template
        .replace("YYYY", today.strftime("%Y"))
        .replace("MM", today.strftime("%m"))
        .replace("DD", today.strftime("%d"))
    )


def get_matching_file(files, prefix, suffix, src_folder_path, resolved_quarantine):
    """
    Return exactly one matching file.
    If more than one matching file is found, all are quarantined.
    If any other file exists that does NOT match the expected prefix/suffix,
    quarantine it as INVALID FILE.
    """

    matching = []
    invalid_files = []

    for f in files:
        name = f.name.lower()

        # Check if file matches expected pattern
        if name.startswith(prefix.lower()) and name.endswith(suffix.lower()):
            matching.append(f)
        else:
            invalid_files.append(f)

    # --- INVALID FILES FOUND ---
    if invalid_files:
        logger.error(f"Invalid files detected: {[f.name for f in invalid_files]}")
        mssparkutils.fs.mkdirs(resolved_quarantine)

        for f in invalid_files:
            src = f"{src_folder_path}{f.name}"
            dest = f"{resolved_quarantine}/{f.name}"
            logger.error(f"Quarantining INVALID FILE: {f.name}")
            mssparkutils.fs.mv(src, dest)
            
        raise RuntimeError("Invalid file(s) found — moved to quarantine")

    # --- No matching files ---
    if not matching:
        raise FileNotFoundError("No matching files found in source folder.")

    # --- More than one matching file ---
    if len(matching) > 1:
        logger.error(f"Multiple matching files found ({len(matching)}). Quarantining all.")

        mssparkutils.fs.mkdirs(resolved_quarantine)

        for f in matching:
            src = f"{src_folder_path}{f.name}"
            dest = f"{resolved_quarantine}/{f.name}"
            logger.error(f"Quarantining file: {f.name}")
            mssparkutils.fs.mv(src, dest)

        raise RuntimeError(
            f"Multiple matching files found — all moved to quarantine: {resolved_quarantine}"
        )

    # --- Exactly one valid file ---
    return matching[0]

def audit_insert_received(batch_id, file_name, file_path, file_size, source_system, pipeline_run_id):
    df = spark.createDataFrame(
        [(batch_id, file_name, file_path, file_size, "RECEIVED",
          source_system, datetime.now(), pipeline_run_id)],
        ["BATCH_ID", "FILE_NAME", "FILE_PATH", "FILE_SIZE",
         "STATUS", "SOURCE_SYSTEM", "FILE_ARRIVAL_UTC", "PIPELINE_RUN_ID"]
    )

    df.write.format("delta").mode("append").saveAsTable(file_valid_tbl)

def audit_mark_duplicate(batch_id, file_name, file_path):
    dt = DeltaTable.forName(spark, file_valid_tbl)
    dt.update(
        condition=f"FILE_NAME = '{file_name}' AND FILE_PATH = '{file_path}'",
        set={"STATUS": lit('DUPLICATE')}
    )


def audit_mark_processed(batch_id, file_name):
    dt = DeltaTable.forName(spark, file_valid_tbl)

    dt.update(
        condition=f"BATCH_ID = '{batch_id}' AND FILE_NAME = '{file_name}'",
        set={"STATUS": lit("PROCESSED")}
    )


def audit_mark_quarantine(batch_id, file_name):
    dt = DeltaTable.forName(spark, file_valid_tbl)

    dt.update(
        condition=f"BATCH_ID = '{batch_id}' AND FILE_NAME = '{file_name}'",
        set={"STATUS": lit("QUARANTINE")}
    )

def file_format_check(files, expected_extension, src_folder_path, resolved_quarantine):
    """
    Quarantine any file whose extension does NOT match the expected extension.
    Example: expected_extension = '.csv'
    """
    unexpected_found = False
    for f in files:
        file_name = f.name.lower()

        if not file_name.endswith(expected_extension.lower()):
            logger.error(f"Unexpected file format found: {f.name}")

            mssparkutils.fs.mkdirs(resolved_quarantine)
            mssparkutils.fs.mv(
                f"{src_folder_path}{f.name}",
                f"{resolved_quarantine}/{f.name}"
            )
            unexpected_found = True
            audit_mark_quarantine(batch_id, selected_file.name)

    # Raise AFTER loop so all invalid files are quarantined
    if unexpected_found:
        raise RuntimeError("Unexpected file format(s) found — all invalid files moved to quarantine")


def validate_file_size(selected_file, src_folder_path, resolved_quarantine, threshold_bytes):
    file_size = selected_file.size
    logger.info(f"File size: {file_size} bytes")

    # --- If threshold is None or empty -- only check empty file ---
    if threshold_bytes is None or str(threshold_bytes).strip() == "":
        logger.info("Threshold is NULL/EMPTY → only checking empty file")

        if file_size == 0:
            reason = "EMPTY FILE"
            logger.error(reason)
            mssparkutils.fs.mkdirs(resolved_quarantine)
            mssparkutils.fs.mv(f"{src_folder_path}{selected_file.name}",
                               f"{resolved_quarantine}/{selected_file.name}")
            audit_mark_quarantine(batch_id, selected_file.name)
            raise RuntimeError(f"{reason} — moved to quarantine")

        return True

    # Convert threshold to int (in case it comes as string)
    threshold_bytes = int(threshold_bytes)

    # --- Full validation when threshold is provided ---
    if file_size == 0:
        reason = "EMPTY FILE"
    elif file_size < threshold_bytes:
        reason = f"FILE TOO SMALL (< {threshold_bytes} bytes)"
    else:
        return True

    # Quarantine logic
    logger.error(reason)
    mssparkutils.fs.mkdirs(resolved_quarantine)
    mssparkutils.fs.mv(f"{src_folder_path}{selected_file.name}",
                       f"{resolved_quarantine}/{selected_file.name}")
    raise RuntimeError(f"{reason} — moved to quarantine")


# ---------------------------------------------------------
# Read Function (with row-count + header logic + quarantine)
# ---------------------------------------------------------
def read_file(spark, source_format, header, delimiter, src_folder_path,resolved_quarantine,selected_file,file_path):
    """
    Read a file using Spark with dynamic options and validate record count:

      - If header = TRUE  → require >= 2 rows (header + 1 data row)
      - If header = FALSE → require >= 1 row (at least 1 data row)
      - Validation uses df.limit(2).count() for performance
      - On failure → raise exception and move file to quarantine
    """
    try:
        logger.info(f"Reading file: {file_path}")

        reader = spark.read.format(source_format)

        if source_format.lower() == "csv":
            reader = reader.option("header", header)
            if delimiter is not None:
                reader = reader.option("delimiter", delimiter)

        # Load DF
        df = reader.load(file_path)
        logger.info("File read successfully")

        # FAST ROW COUNT CHECK (limit to 2 rows)

        record_count = df.count()
        logger.info(f"Fast row-count result = {record_count}")

        # Normalize header to boolean
        header_flag = False
        if isinstance(header, str):
            header_flag = header.strip().lower() == "true"
        elif isinstance(header, bool):
            header_flag = header

        # Case 1: header = TRUE  → need at least 2 rows
        if header_flag:
            if record_count < 2:
                reason = (
                    f"INSUFFICIENT ROWS — only {record_count} row(s) found. "
                    "Expected at least 2 (header + 1 data row)."
                )
                logger.error(reason)
                mssparkutils.fs.mkdirs(resolved_quarantine)
                mssparkutils.fs.mv(f"{src_folder_path}{selected_file.name}",
                                   f"{resolved_quarantine}/{selected_file.name}")
                audit_mark_quarantine(batch_id, selected_file.name)                   
                raise RuntimeError(f"{reason} — moved to quarantine")

        # Case 2: header = FALSE → need at least 1 row
        else:
            if record_count == 0:
                reason = (
                    "NO DATA ROWS — file contains 0 rows. "
                    "Expected at least 1 data row."
                )
                logger.error(reason)
                mssparkutils.fs.mkdirs(resolved_quarantine)
                mssparkutils.fs.mv(f"{src_folder_path}{selected_file.name}",
                                   f"{resolved_quarantine}/{selected_file.name}")
                audit_mark_quarantine(batch_id, selected_file.name)
                raise RuntimeError(f"{reason} — moved to quarantine")

        return df, record_count

    except Exception as ex:
        logger.error(f"Error reading file: {ex}")
        raise


# ---------------------------------------------------------
# Write Function
# ---------------------------------------------------------
def write_table(df, target_format, write_mode, target_schema, target_table_name):
    """Write DataFrame to a target table."""
    try:
        logger.info(f"Writing to table {target_schema}.{target_table_name}")

        # Count records before writing
        target_count = df.count()

        df.write \
            .format(target_format) \
            .mode(write_mode) \
            .saveAsTable(f"{target_schema}.{target_table_name}")

        logger.info("Write completed successfully")

        return target_count

    except Exception as ex:
        logger.error(f"Error writing to table: {ex}")
        raise
       
# ---------------------------------------------------------
# Validate Function
# ---------------------------------------------------------
from pyspark.sql.functions import lit
from datetime import datetime

def validate_schema(df, source_object_name, transformed_name):
    """
    Validate column names and order against control_schema_metadata.
    Returns True if validation passed, False if failed.
    """

    logger.info("Validating column names and order using control_schema_metadata")

    metadata_df = (
        spark.table("control.cfg_expected_schema")
             .filter(f"FILE_NAME = '{source_object_name}'")
             .orderBy("COLUMN_POSITION")
    )

    expected_cols = [row["COLUMN_NAME"] for row in metadata_df.collect()]
    actual_cols = df.columns

    results = []
    errors_found = False
    source_type = "File"

    audit_schema = StructType([
        StructField("BATCH_ID", StringType(), True),
        StructField("SOURCE_TYPE", StringType(), True),
        StructField("SOURCE_NAME", StringType(), True),
        StructField("VALIDATION_STAGE", StringType(), True),
        StructField("COLUMN_POSITION", IntegerType(), True),
        StructField("EXPECTED_COLUMN_NAME", StringType(), True),
        StructField("ACTUAL_COLUMN_NAME", StringType(), True),
        StructField("EXPECTED_DATA_TYPE", StringType(), True),
        StructField("ACTUAL_DATA_TYPE", StringType(), True),
        StructField("VALIDATION_STATUS", StringType(), True),
        StructField("VALIDATION_MESSAGE", StringType(), True),
        StructField("CREATED_DATE", TimestampType(), True)
    ])

    # 1. Column count check
    if len(expected_cols) != len(actual_cols):
        errors_found = True
        results.append({
            "BATCH_ID": batch_id,
            "SOURCE_TYPE": "File",
            "SOURCE_NAME": transformed_name,
            "VALIDATION_STAGE": "FILE COLUMN COUNT CHECK",
            "COLUMN_POSITION": None,
            "EXPECTED_COLUMN_NAME": None,
            "ACTUAL_COLUMN_NAME": None,
            "EXPECTED_DATA_TYPE": None,
            "ACTUAL_DATA_TYPE": None,
            "VALIDATION_STATUS": "FAIL",
            "VALIDATION_MESSAGE": (
                f"Column count mismatch: expected {len(expected_cols)}, "
                f"received {len(actual_cols)}"
            ),
            "CREATED_DATE": datetime.now()
        })

    # 2. Column name + order check
    for i, expected in enumerate(expected_cols):
        actual = actual_cols[i] if i < len(actual_cols) else None

        if actual != expected:
            errors_found = True
            status = "FAIL"
            message = f"Position {i+1}: expected '{expected}', received '{actual}'"
        else:
            status = "PASS"
            message = "Column name and position match"

        results.append({
            "BATCH_ID": batch_id,
            "SOURCE_TYPE": source_type,
            "SOURCE_NAME": transformed_name,
            "VALIDATION_STAGE": "FILE COLUMN NAME CHECK",
            "COLUMN_POSITION": i + 1,
            "EXPECTED_COLUMN_NAME": expected,
            "ACTUAL_COLUMN_NAME": actual,
            "EXPECTED_DATA_TYPE": None,
            "ACTUAL_DATA_TYPE": None,
            "VALIDATION_STATUS": status,
            "VALIDATION_MESSAGE": message,
            "CREATED_DATE": datetime.now()
        })

    # Write audit results
    results_df = spark.createDataFrame(results, schema=audit_schema)
    results_df.write.format("delta").mode("append").saveAsTable("audit.ops_schema_validation_results")

    return not errors_found




# ---------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------
def main():
    spark = SparkSession.builder.appName("Bronze_staging_load").getOrCreate()
    today = datetime.now()
    try:
        logger.info("Starting Bronze staging load process")

        # Normalize delimiter
        normalized_delimiter = normalize_delimiter(delimiter)

        # Resolve file name
        transformed_name = resolve_filename(source_object_name, today)
        logger.info(f"Resolved file name: {transformed_name}")

        # Extract prefix and suffix
        lower_name = transformed_name.lower()
        file_prefix = transformed_name.split(".", 1)[0]
        file_suffix = "." + lower_name.split(".", 1)[1]

        logger.info(f"File prefix: {file_prefix}")
        logger.info(f"File suffix: {file_suffix}")

        # Resolve folder path
        src_folder_path = resolve_path(source_path, today)
        resolved_quarantine = resolve_path(quarantine_path, today)
        resolved_processed_path   = resolve_path(processed_path, today)
        logger.info(f"Source folder path: {src_folder_path}")


        
        # List files in folder
        try:
            files = mssparkutils.fs.ls(src_folder_path)
        except Exception as e:
            raise FileNotFoundError(f"Source path not found: {src_folder_path}") from e        

        logger.info(f"Files found: {[f.name for f in files]}")

        # Check if the files are in expected format
        expected_extension = source_format  
        file_format_check(
        files,
        expected_extension,
        src_folder_path,
        resolved_quarantine
        )
        
        # Find matching file
        selected_file = get_matching_file(
                                           files,
                                           file_prefix,
                                           file_suffix,
                                           src_folder_path,
                                           resolved_quarantine
                                        )

        logger.info(f"Selected file: {selected_file.name}")

        final_source_path = f"{src_folder_path}{selected_file.name}"
        logger.info(f"Final source path: {final_source_path}")
        
        pipeline_run_id = mssparkutils.env.getJobId()

        audit_insert_received(
            batch_id=batch_id,
            file_name=selected_file.name,
            file_path=final_source_path,
            file_size=selected_file.size,
            source_system=source_system,
            pipeline_run_id=pipeline_run_id
        )

        # Check for duplicate file
        existing = spark.sql(f"""
            SELECT 1 FROM {file_valid_tbl}
            WHERE FILE_NAME = '{selected_file.name}'
              AND STATUS IN ('RECEIVED','PROCESSED','DUPLICATE')
        """).count()

        if existing > 1:  # RECEIVED + OLD RECORD = DUPLICATE
            audit_mark_duplicate(batch_id, selected_file.name, final_source_path)

            mssparkutils.fs.mkdirs(resolved_quarantine)
            mssparkutils.fs.mv(final_source_path, f"{resolved_quarantine}/{selected_file.name}")

            raise RuntimeError(f"Duplicate file detected: {selected_file.name} → moved to quarantine")

        validate_file_size(
            selected_file=selected_file,
            src_folder_path=src_folder_path,
            resolved_quarantine=resolved_quarantine,
            threshold_bytes=file_size_min_threshold
        )
        
        # Read file
        df_raw, record_count = read_file(
            spark,
            source_format,
            source_file_header,
            normalized_delimiter,
            src_folder_path,
            resolved_quarantine,
            selected_file,
            final_source_path
        )

        # Call validation
        is_valid = validate_schema(df_raw, source_object_name, transformed_name)

        if not is_valid:
            final_quarantine_path = f"{resolved_quarantine}/{selected_file.name}"
            logger.error(f"Schema validation failed. Moving file to quarantine: {final_quarantine_path}")

            # Ensure quarantine folder exists
            mssparkutils.fs.mkdirs(resolved_quarantine)
            mssparkutils.fs.mv(final_source_path, final_quarantine_path)
            raise RuntimeError("Schema validation failed — file moved to quarantine")  

        # Add metadata column
        df_raw = (df_raw
                       .withColumn("BATCH_ID", lit(batch_id))
                       .withColumn("FILE_NAME", lit(selected_file.name))                  
                       .withColumn("LOAD_DATE", lit(current_timestamp()))
                       .withColumn("SOURCE_SYSTEM", lit(source_system))
                )

        # Write to target
        target_count =0
        target_count =  write_table(
            df_raw,
            target_format,
            target_write_mode,
            target_schema,
            target_table_name
        )
       
        if history_load_indicator== 'Y':
            df_history = (df_raw
                          .withColumn("HISTORY_ID", expr("uuid()"))
                          .withColumn("BATCH_ID", lit(batch_id))
                          .withColumn("FILE_NAME", lit(selected_file.name))                  
                          .withColumn("LOAD_DATE", lit(current_timestamp()))
                          .withColumn("SOURCE_SYSTEM", lit(source_system))
                )
            write_table(
                df_history,
                history_target_format,
                history_target_write_mode,
                history_target_schema,
                history_target_table_name
            )
          
        # Move file to processed path
        mssparkutils.fs.mkdirs(resolved_processed_path)
        mssparkutils.fs.mv(final_source_path, f"{resolved_processed_path}/{selected_file.name}")


        logger.info("Pipeline completed successfully")
        audit_mark_processed(batch_id, selected_file.name)
        
        # AUDIT TABLE INSERT
        audit_df = spark.createDataFrame(
            [(batch_id,
              source_object_name,
              target_table_name,
              datetime.now(),
              record_count,       # SOURCE_RECORDS
              target_count,       # INSERT_RECORDS (Bronze always inserts)
              0,                  # UPDATE_RECORDS (Bronze never updates)
              0                   # DELETE_RECORDS (Bronze never deletes)
             )],
            ["BATCH_ID",
             "SOURCE_TABLE",
             "TARGET_TABLE",
             "CREATED_DATE",
             "SOURCE_RECORDS",
             "INSERT_RECORDS",
             "UPDATE_RECORDS",
             "DELETE_RECORDS"]
        )

        audit_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(audit_tbl)

        logger.info(
            f"Audit written: batch={batch_id}, "
            f"source_records={record_count}, target_records={target_count}"
        )
 
    except FileNotFoundError as fnf:
        logger.error(f"File or path not found: {fnf}")
        raise Exception(f"Unexpected error occurred: {str(fnf)}")

    except RuntimeError as re:
        logger.error(f"Validation error: {re}")
        raise Exception(f"Unexpected error occurred: {str(re)}")

    except Exception as ex:
        logger.error(f"Unexpected error occurred: {ex}")
        raise Exception(f"Unexpected error occurred: {str(ex)}")


if __name__ == "__main__":
    main()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
