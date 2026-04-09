# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql.functions import array, lit, explode, struct, col
from datetime import datetime, timedelta

df = (
     spark.table("control.cfg_bronze_staging") 
    .select("ROOT_SOURCE_PATH","ROOT_PROCESSED_PATH", "ROOT_QUARANTINE_PATH","RETENTION_DAYS")         
    .filter(col("ISACTIVE") == 'Y')
    .filter(col("INTERFACE_ID") == interface_id)
)

# Unpivot the 3 path columns into 3 rows
result_df = (
    df.select(
        explode(
            array(
                struct(col("ROOT_SOURCE_PATH").alias("ROOT_PATH"), col("RETENTION_DAYS")),
                struct(col("ROOT_PROCESSED_PATH").alias("ROOT_PATH"), col("RETENTION_DAYS")),
                struct(col("ROOT_QUARANTINE_PATH").alias("ROOT_PATH"), col("RETENTION_DAYS"))
            )
        ).alias("row")
    )
    .select("row.ROOT_PATH", "row.RETENTION_DAYS")
    .distinct()
)

result_df.show(truncate=False)

from notebookutils import mssparkutils
from datetime import datetime, timedelta

def safe_ls(path):
    try:
        return mssparkutils.fs.ls(path)
    except:
        return []

def safe_rm(path, recurse=False):
    try:
        mssparkutils.fs.rm(path, recurse=recurse)
    except:
        pass   # ignore missing paths

def clean_folder(path, cutoff, ROOT, all_deleted_files, all_deleted_folders):

    items = safe_ls(path)  # Returns files and sub folders
    print("items:", items)
    

    for item in items: #Check whether item is folder or file

        if item.isDir: #if folder again start from top
            clean_folder(item.path, cutoff, ROOT, all_deleted_files, all_deleted_folders)

        else: #if file perform deletion of file
            file_time = datetime.fromtimestamp(item.modifyTime / 1000)

            if file_time <= cutoff:
                print(f"Deleting file: {item.path}")
                safe_rm(item.path)
                all_deleted_files.append(item.path)

    # delete folder only if NOT the root and folder is empty
    if path != ROOT and len(safe_ls(path)) == 0:
        print(f"Deleting empty folder: {path}")
        safe_rm(path, recurse=True)
        all_deleted_folders.append(path)



# ---------------- MAIN LOOP ----------------

control_rows = result_df.collect()

all_deleted_files = []
all_deleted_folders = []

for row in control_rows:

    base_path = row["ROOT_PATH"].rstrip("/")
    retention_days = row["RETENTION_DAYS"]

    # For  retention:
    cutoff = datetime.now() - timedelta(days=retention_days)

    print(f"\nProcessing root: {base_path}")
    print(f"Cutoff: {cutoff}")

    clean_folder(base_path, cutoff, base_path, all_deleted_files, all_deleted_folders)


print("\nDeleted files:", len(all_deleted_files))
print("Deleted folders:", len(all_deleted_folders))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
