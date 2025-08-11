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
base_path = f"Files/Sharepoint/ChamCongChiTiet/{today_path}"

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

df = spark.sql("SELECT * FROM LKH_Bronze_Data.Watermark LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
