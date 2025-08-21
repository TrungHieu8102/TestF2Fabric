# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4b57cc09-e0a2-4ffd-80a0-4520f65d78f2",
# META       "default_lakehouse_name": "LKH_Bronze_Data",
# META       "default_lakehouse_workspace_id": "8c76cb1c-cf62-4ebe-b9df-299509803689",
# META       "known_lakehouses": [
# META         {
# META           "id": "4b57cc09-e0a2-4ffd-80a0-4520f65d78f2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import lit

# Lấy ngày hiện tại để sử dụng trong đường dẫn
today_path = datetime.utcnow().strftime('%Y/%m/%d')
base_path = f"Files/Sharepoint/ChamCong/{today_path}"

# Liệt kê các file trong thư mục
files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).listStatus(
    spark._jvm.org.apache.hadoop.fs.Path(base_path)
)

files_list = [file.getPath().toString() for file in files]

# Lấy file mới nhất dựa theo thời gian sửa đổi
latest_file = max(files_list, key=lambda x: spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()).getFileStatus(spark._jvm.org.apache.hadoop.fs.Path(x)).getModificationTime())
    
# Đọc dữ liệu parquet từ file mới nhất
df = spark.read.parquet(latest_file)

# Tính giá trị max(modified) từ file Bronze
max_modified = df.agg({"Modified": "max"}).collect()[0][0]

# Đường dẫn bảng watermark
watermark_path = "abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/4b57cc09-e0a2-4ffd-80a0-4520f65d78f2/Tables/Watermark"
watermark_table = DeltaTable.forPath(spark, watermark_path)

# Xóa dữ liệu trong bảng watermark bằng cách ghi đè với DataFrame rỗng có schema
watermark_table.delete()

# Chèn giá trị max(modified) vào bảng watermark
new_watermark_df = spark.createDataFrame([(max_modified,)], ["Modified"])
new_watermark_df.write.format("delta").mode("append").save(watermark_path)

# Hiển thị kết quả DataFrame
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from datetime import datetime
import logging

# --- 1. Cấu hình ---
def setup_configuration(table_list_str: str) -> tuple:
    """Cấu hình các tham số cần thiết."""
    lakehouse_bronze_path = '8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/4b57cc09-e0a2-4ffd-80a0-4520f65d78f2'
    tables = [table.strip() for table in table_list_str.split(",") if table.strip()]
    today_path = datetime.utcnow().strftime('%Y/%m/%d')
    return lakehouse_bronze_path, tables, today_path

# --- 2. Hàm lấy file parquet mới nhất ---
def get_latest_parquet_path(spark: SparkSession, base_path: str) -> str:
    """Lấy đường dẫn file Parquet mới nhất từ thư mục."""
    try:
        # Sử dụng Spark để liệt kê file, thay vì Hadoop FileSystem trực tiếp
        df_files = spark.read.format("parquet").option("recursiveFileLookup", "true").load(base_path)
        files_list = [row.path for row in spark.sparkContext.wholeTextFiles(base_path).collect() if row.path.endswith(".parquet")]
        
        if not files_list:
            logging.warning(f"Không tìm thấy file Parquet trong {base_path}")
            return None
        
        # Sử dụng Spark SQL để tìm file mới nhất
        df = spark.createDataFrame([(f,) for f in files_list], ["path"])
        latest_file = df.select("path").orderBy(col("path").desc()).limit(1).collect()[0][0]
        return latest_file
    except Exception as e:  
        logging.error(f"Lỗi khi đọc thư mục {base_path}: {str(e)}")
        return None

# --- 3. Hàm merge dữ liệu vào bảng Silver ---
def merge_to_silver(spark: SparkSession, df: 'DataFrame', table_name: str, merge_condition: str) -> None:
    """Thực hiện merge dữ liệu từ Bronze vào Silver."""
    try:
        if not spark.catalog.tableExists(table_name):
            logging.info(f"Tạo mới bảng Silver: {table_name}")
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        else:
            logging.info(f"Merge vào bảng Silver: {table_name}")
            delta_table_silver = DeltaTable.forName(spark, table_name)
            delta_table_silver.alias("silver").merge(
                source=df.alias("bronze"),
                condition=merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logging.info(f"Hoàn thành xử lý bảng {table_name}")
    except Exception as e:
        logging.error(f"Lỗi khi xử lý bảng {table_name}: {str(e)}")
        raise

# --- 4. Hàm chính ---
def process_bronze_to_silver(spark: SparkSession, table_list_str: str, merge_condition: str = "silver.ID = bronze.ID") -> None:
    """Xử lý dữ liệu từ Bronze sang Silver cho danh sách bảng."""
    # Thiết lập logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Lấy cấu hình
    lakehouse_bronze_path, tables, today_path = setup_configuration(table_list_str)
    
    if not tables:
        logging.error("Danh sách bảng rỗng. Kết thúc xử lý.")
        return

    # Xử lý từng bảng
    for table_name in tables:
        logging.info(f"Bắt đầu xử lý bảng: {table_name}")
        base_path = f"abfss://{lakehouse_bronze_path}/Files/incremental_loads/{table_name}/{today_path}"
        
        # Lấy file Parquet mới nhất
        latest_file = get_latest_parquet_path(spark, base_path)
        if not latest_file:
            logging.warning(f"Không tìm thấy file Parquet cho bảng {table_name}")
            continue
        
        logging.info(f"File mới nhất: {latest_file}")
        
        # Đọc dữ liệu
        try:
            df = spark.read.parquet(latest_file).cache()  # Cache để tối ưu hiệu suất
            merge_to_silver(spark, df, table_name, merge_condition)
            df.unpersist()  # Giải phóng cache
        except Exception as e:
            logging.error(f"Lỗi khi đọc hoặc merge bảng {table_name}: {str(e)}")
            continue

# --- 5. Thực thi ---
if __name__ == "__main__":
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    table_list = "table1,table2,table3"  # Thay bằng danh sách bảng thực tế
    process_bronze_to_silver(spark, table_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
