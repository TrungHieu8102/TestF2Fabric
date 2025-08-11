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
# META         },
# META         {
# META           "id": "41c8c31a-0f05-4bea-aad4-9b18da24a1b3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

##Upsert bảng Chấm Công

from datetime import datetime

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

from delta.tables import DeltaTable

# Đường dẫn bảng Delta Silver
silver_table_path = "abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/41c8c31a-0f05-4bea-aad4-9b18da24a1b3/Tables/Ierp_ChamCong"
silver_table = DeltaTable.forPath(spark, silver_table_path)

# Điều kiện upsert theo TimekeepingID
upsert_condition = "bronze_data.TimekeepingID = silver_data.TimekeepingID"

# Thực hiện upsert với các cột đúng theo bảng
silver_table.alias("silver_data").merge(
    df.alias("bronze_data"),
    upsert_condition
).whenMatchedUpdate(
    condition = "bronze_data.Modified > silver_data.Modified",
    set = {
        "Id": "bronze_data.Id",
        "ContentTypeID": "bronze_data.ContentTypeID",
        "ContentType": "bronze_data.ContentType",
        "TimekeepingID": "bronze_data.TimekeepingID",
        "Modified": "bronze_data.Modified",
        "Created": "bronze_data.Created",
        "CreatedById": "bronze_data.CreatedById",
        "ModifiedById": "bronze_data.ModifiedById",
        "Owshiddenversion": "bronze_data.Owshiddenversion",
        "Version": "bronze_data.Version",
        "Path": "bronze_data.Path",
        "ComplianceAssetId": "bronze_data.ComplianceAssetId",
        "StatusValue": "bronze_data.StatusValue",
        "ManagerNote": "bronze_data.ManagerNote",
        "Note": "bronze_data.Note",
        "IsOvertime": "bronze_data.IsOvertime",
        "Week": "bronze_data.Week",
        "Year": "bronze_data.Year",
        "EmployeeId": "bronze_data.EmployeeId",
        "DAVVId": "bronze_data.DAVVId",
        "DAVVName": "bronze_data.DAVVName",
        "ManagerEmail": "bronze_data.ManagerEmail",
        "EmployeeName": "bronze_data.EmployeeName",
        "ApproverEmail": "bronze_data.ApproverEmail",
        "IsOnsite": "bronze_data.IsOnsite",
        "MealAllowance": "bronze_data.MealAllowance",
        "TransportationAllowance": "bronze_data.TransportationAllowance",
        "ApproverNameId": "bronze_data.ApproverNameId",
        "LTDLAllowance": "bronze_data.LTDLAllowance",
        "ColorTag": "bronze_data.ColorTag"
    }
).whenNotMatchedInsert(
    values = {
        "Id": "bronze_data.Id",
        "ContentTypeID": "bronze_data.ContentTypeID",
        "ContentType": "bronze_data.ContentType",
        "TimekeepingID": "bronze_data.TimekeepingID",
        "Modified": "bronze_data.Modified",
        "Created": "bronze_data.Created",
        "CreatedById": "bronze_data.CreatedById",
        "ModifiedById": "bronze_data.ModifiedById",
        "Owshiddenversion": "bronze_data.Owshiddenversion",
        "Version": "bronze_data.Version",
        "Path": "bronze_data.Path",
        "ComplianceAssetId": "bronze_data.ComplianceAssetId",
        "StatusValue": "bronze_data.StatusValue",
        "ManagerNote": "bronze_data.ManagerNote",
        "Note": "bronze_data.Note",
        "IsOvertime": "bronze_data.IsOvertime",
        "Week": "bronze_data.Week",
        "Year": "bronze_data.Year",
        "EmployeeId": "bronze_data.EmployeeId",
        "DAVVId": "bronze_data.DAVVId",
        "DAVVName": "bronze_data.DAVVName",
        "ManagerEmail": "bronze_data.ManagerEmail",
        "EmployeeName": "bronze_data.EmployeeName",
        "ApproverEmail": "bronze_data.ApproverEmail",
        "IsOnsite": "bronze_data.IsOnsite",
        "MealAllowance": "bronze_data.MealAllowance",
        "TransportationAllowance": "bronze_data.TransportationAllowance",
        "ApproverNameId": "bronze_data.ApproverNameId",
        "LTDLAllowance": "bronze_data.LTDLAllowance",
        "ColorTag": "bronze_data.ColorTag"
    }
).execute()

# Hiển thị kết quả dataframe
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
