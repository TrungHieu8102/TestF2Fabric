# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "78c6f81c-d512-439b-874c-2e8d7af8b9ae",
# META       "default_lakehouse_name": "LKH_Silver_Test",
# META       "default_lakehouse_workspace_id": "8c76cb1c-cf62-4ebe-b9df-299509803689",
# META       "known_lakehouses": [
# META         {
# META           "id": "4b57cc09-e0a2-4ffd-80a0-4520f65d78f2"
# META         },
# META         {
# META           "id": "78c6f81c-d512-439b-874c-2e8d7af8b9ae"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
from delta.tables import DeltaTable
from datetime import datetime
import json

# Config
lakehouse_bronze_path = '8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/4b57cc09-e0a2-4ffd-80a0-4520f65d78f2'
today_path = datetime.utcnow().strftime('%Y/%m/%d')
# RUN_ID = '149d9d4f-583f-428a-a80d-3e4f2c0328a2'
MERGE_KEY_COL = "Id"

def process_one_table(tbl: str):
    base_path = f"abfss://{lakehouse_bronze_path}/Files/data_loaded/{tbl}/{today_path}/{RUN_ID}"
    df = (spark.read
              .option("recursiveFileLookup", "true")
              .parquet(f"{base_path}/*.parquet"))

    if not spark.catalog.tableExists(tbl):
        (df.write.format("delta").mode("overwrite")
           .option("mergeSchema", "true").saveAsTable(tbl))
        print(f"[CREATED] {tbl}")
        return

    if MERGE_KEY_COL not in df.columns:
        raise ValueError(f"{tbl}: missing merge key '{MERGE_KEY_COL}'")

    delta_tbl = DeltaTable.forName(spark, tbl)
    (df.limit(0).write.format("delta").mode("append")
       .option("mergeSchema", "true").saveAsTable(tbl))

    cond = f"silver.`{MERGE_KEY_COL}` = bronze.`{MERGE_KEY_COL}`"
    (delta_tbl.alias("silver")
        .merge(source=df.alias("bronze"), condition=cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"[MERGED] {tbl}")

# ---- Parse đầu vào 'table_name' từ Pipeline (JSON string) và chạy ----
if isinstance(table_name, str) and table_name.strip().startswith('['):
    tables = json.loads(table_name)           # chuyển từ chuỗi JSON -> list[str]
elif isinstance(table_name, list):
    tables = table_name                        # đã là list
else:
    tables = [str(table_name)]                 # 1 giá trị đơn

for t in [str(x).strip() for x in tables if str(x).strip()]:
    process_one_table(t)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Welcome to your new notebook
from delta.tables import DeltaTable
from datetime import datetime
import json

# --- Config ---
lakehouse_bronze_path = '8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/4b57cc09-e0a2-4ffd-80a0-4520f65d78f2'
today_path = datetime.utcnow().strftime('%Y/%m/%d')
MERGE_KEY_COL = "Id"

# --- Lấy đường dẫn file parquet mới nhất (duyệt đệ quy trong thư mục theo ngày) ---
def latest_parquet_path(day_dir: str):
    hconf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
    Path = spark._jvm.org.apache.hadoop.fs.Path
    if not fs.exists(Path(day_dir)):
        return None

    files = []
    def walk(p):
        for st in fs.listStatus(p):
            if st.isFile() and st.getPath().getName().lower().endswith(".parquet"):
                files.append(st.getPath())
            elif st.isDirectory():
                walk(st.getPath())
    walk(Path(day_dir))

    if not files:
        return None
    latest = max(files, key=lambda p: fs.getFileStatus(p).getModificationTime())
    return latest.toString()

def process_one_table(tbl: str):
    # Duyệt trong thư mục theo NGÀY: .../{tbl}/{YYYY/MM/DD}/(các RunID)
    day_root = f"abfss://{lakehouse_bronze_path}/Files/data_loaded/{tbl}/{today_path}/{RUN_ID}"
    latest_file = latest_parquet_path(day_root)
    if not latest_file:
        print(f"[SKIP] {tbl} - no parquet under {day_root}")
        return

    df = spark.read.parquet(latest_file)

    if not spark.catalog.tableExists(tbl):
        (df.write.format("delta").mode("overwrite")
           .option("mergeSchema", "true").saveAsTable(tbl))
        print(f"[CREATED] {tbl}")
        return

    if MERGE_KEY_COL not in df.columns:
        raise ValueError(f"{tbl}: missing merge key '{MERGE_KEY_COL}'")

    delta_tbl = DeltaTable.forName(spark, tbl)
    # Cho phép mở rộng schema đích nếu nguồn có cột mới
    (df.limit(0).write.format("delta").mode("append")
       .option("mergeSchema", "true").saveAsTable(tbl))

    cond = f"silver.`{MERGE_KEY_COL}` = bronze.`{MERGE_KEY_COL}`"
    (delta_tbl.alias("silver")
        .merge(source=df.alias("bronze"), condition=cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"[MERGED] {tbl}")

# --- Parse 'table_name' từ Pipeline (JSON string hoặc list) ---
if isinstance(table_name, str) and table_name.strip().startswith('['):
    tables = json.loads(table_name)
elif isinstance(table_name, list):
    tables = table_name
else:
    tables = [str(table_name)]

for t in [str(x).strip() for x in tables if str(x).strip()]:
    process_one_table(t)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

db = "LKH_Silver_Test"
for t in spark.catalog.listTables(db):
    spark.sql(f"DROP TABLE IF EXISTS `{db}`.`{t.name}`")
print("Done.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
