# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, to_date,count,length
from pyspark.sql.types import StructType, StructField, StringType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def EmptyValueCheck(df, column_name, record_identifier_col):
    schema = StructType([
    StructField("record_identifier_col", StringType(), True),
    StructField("column_name", StringType(), True)
])
    # Filter rows where column is empty string
    empty_df = df.filter(length(trim(col(column_name))) == 0) \
                 .select(col(record_identifier_col).alias("record_identifier_col"),lit(column_name).alias("column_name"))
    # Count rows safely
    row_count = empty_df.count()
    # Return empty DataFrame instead of None
    if row_count >= 1:
        empty_df.printSchema()
        return empty_df
    else:
     empty_df = spark.createDataFrame([], schema)
     return empty_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def DuplicatesCheck(df,  column_name,  record_identifier_col):

    schema = StructType([
        StructField("record_identifier_col", StringType(), True),
        StructField("column_name", StringType(), True)
    ])

    df_filtered = df.filter(
        (col(column_name).isNotNull()) &
        (trim(col(column_name)) != "")
    )
 
    # Step 1: Find values that appear more than once
    duplicate_groups = (
        df_filtered.groupBy(column_name)
          .agg(count("*").alias("cnt"))
          .filter(col("cnt") > 1)
          .select(column_name)
    )
 
    # Step 2: Join back to get the actual duplicate rows
    duplicate_df = (
        df_filtered.join(duplicate_groups, on=column_name, how="inner")
          .select(
              col(record_identifier_col).alias("record_identifier_col"),
              lit(column_name).alias("column_name")
          )
    )
 
    # Step 3: Count duplicates
    row_count = duplicate_df.count()
 
    # Step 4: Return non-empty or empty DF with schema
    if row_count >= 1:
        duplicate_df.printSchema()
        return duplicate_df
    else:
        return spark.createDataFrame([], schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def NullValueCheck(df,  column_name, record_identifier_col):
    schema = StructType([
        StructField("record_identifier_col", StringType(), True),
        StructField("column_name", StringType(), True)
    ])

    # Filter rows where column is NULL
    null_df = df.filter(col(column_name).isNull()) \
                .select(
                    col(record_identifier_col).alias("record_identifier_col"),
                    lit(column_name).alias("column_name")
                )

    # Count rows safely
    row_count = null_df.count()

    # Return empty DataFrame instead of None
    if row_count >= 1:
        null_df.printSchema()
        return null_df
    else:
        null_df = spark.createDataFrame([], schema)
        return null_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def SparkDateThresholdCheck(df, column_name, record_identifier_col, cutoff_date="1582-10-16"):

    schema = StructType([
        StructField("record_identifier_col", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("column_value", StringType(), True)
    ])

    invalid_df = (
        df.filter(
            col(column_name).isNotNull() &
            (to_date(col(column_name)) < to_date(lit(cutoff_date)))
        )
        .select(
            col(record_identifier_col).alias("record_identifier_col"),
            lit(column_name).alias("column_name"),
            col(column_name).cast("string").alias("column_value")
        )
    )

    if invalid_df.rdd.isEmpty():
        return spark.createDataFrame([], schema)

    return invalid_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ConditionalNullCheck(df, column_name, record_identifier_col, condition_column):
 
    schema = StructType([
        StructField("record_identifier_col", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("column_value", StringType(), True)
    ])
 
    # Invalid condition:
    # condition_column is NOT NULL
    # AND target column IS NULL
 
    invalid_df = (
        df.filter(
            col(condition_column).isNotNull() &
            col(column_name).isNull()
        )
        .select(
            col(record_identifier_col).alias("record_identifier_col"),
            lit(column_name).alias("column_name"),
            col(column_name).cast("string").alias("column_value")
        )
    )
 
    if invalid_df.rdd.isEmpty():
        return df.sparkSession.createDataFrame([], schema)
 
    return invalid_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def DATERequiredWhenFlagY(df, column_name, record_identifier_col, condition_column):

    flag_name = "DATERequiredWhenFlagY"

    schema = StructType([
        StructField("record_identifier_col", StringType(), True),
        StructField("flag", StringType(), True),
        StructField("column_value", StringType(), True)
    ])

    # Invalid if:
    # flag_column == 'Y'
    # AND date_column IS NULL

    invalid_df = (
        df.filter(
            (col(condition_column) == lit("Y")) &
            col(column_name).isNull()
        )
        .select(
            col(record_identifier_col).alias("record_identifier_col"),
            lit(flag_name).alias("flag"),
            col(column_name).cast("string").alias("column_value")
        )
    )

    if invalid_df.rdd.isEmpty():
        return df.sparkSession.createDataFrame([], schema)

    return invalid_df
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
