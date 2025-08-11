# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "41c8c31a-0f05-4bea-aad4-9b18da24a1b3",
# META       "default_lakehouse_name": "LKH_Silver_Data",
# META       "default_lakehouse_workspace_id": "8c76cb1c-cf62-4ebe-b9df-299509803689",
# META       "known_lakehouses": [
# META         {
# META           "id": "4b57cc09-e0a2-4ffd-80a0-4520f65d78f2"
# META         },
# META         {
# META           "id": "41c8c31a-0f05-4bea-aad4-9b18da24a1b3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col
from delta.tables import DeltaTable
from datetime import datetime
import pytz
print(table_list)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from datetime import datetime, timedelta

# --- 1. Cấu hình các biến ---
# Thay 'your_lakehouse_name' bằng tên Lakehouse thực tế của bạn
lakehouse_bronze_path = '8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/4b57cc09-e0a2-4ffd-80a0-4520f65d78f2'
lakehouse_silver_path = 'abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/41c8c31a-0f05-4bea-aad4-9b18da24a1b3/Tables'

# --- 2. Tìm file Parquet mới nhất ---
try:
    # Lấy ngày hôm qua
    today_path = datetime.utcnow().strftime('%Y/%m/%d')
    
    # Xây dựng đường dẫn đầy đủ đến thư mục với tên Lakehouse
    base_path = f"abfss://{lakehouse_bronze_path}/Files/incremental_loads/{table_name}/{today_path}"
    
    # Liệt kê tất cả các file trong thư mục
    files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).listStatus(
        spark._jvm.org.apache.hadoop.fs.Path(base_path)
    )

    files_list = [file.getPath().toString() for file in files]
    
    if not files_list:
        print(f"Không tìm thấy file nào trong thư mục: {base_path}")
        df = None
    else:
        # Lấy file mới nhất dựa theo thời gian sửa đổi
        latest_file = max(files_list, key=lambda x: spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()).getFileStatus(spark._jvm.org.apache.hadoop.fs.Path(x)).getModificationTime())
        
        print(f"File mới nhất được tìm thấy: {latest_file}")
        
        # Đọc dữ liệu parquet từ file mới nhất
        df = spark.read.parquet(latest_file)

except Exception as e:
    print(f"Lỗi: {e}")
    df = None

# --- 3. Thực hiện MERGE (Upsert) ---
if df:
    try:
        # Kiểm tra xem bảng đích Silver đã tồn tại chưa bằng tên bảng
        if not spark.catalog.tableExists(table_name):
            print(f"Bảng đích {table_name} chưa tồn tại. Tạo mới bảng từ dữ liệu Bronze.")
            # Chỉ truyền tên bảng vào saveAsTable
            df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        else:
            print(f"Bảng đích {table_name} đã tồn tại. Thực hiện MERGE.")

            # Chỉ truyền tên bảng vào forName
            delta_table_silver = DeltaTable.forName(spark, table_name)
            
            merge_condition = f"silver.ID = bronze.ID"
            
            delta_table_silver.alias("silver").merge(
                source = df.alias("bronze"),
                condition = merge_condition
            ).whenMatchedUpdateAll(
            ).whenNotMatchedInsertAll(
            ).execute()
            
        print(f"\nHoàn thành quá trình MERGE cho bảng {table_name}.")

    except Exception as e:
        print(f"\nĐã xảy ra lỗi trong quá trình MERGE: {e}")
else:
    print("\nKhông có dữ liệu mới để xử lý. Quá trình upsert kết thúc.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
