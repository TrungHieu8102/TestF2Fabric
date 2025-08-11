# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6d5c070e-4d6b-4904-9b0f-007e3d70dec6",
# META       "default_lakehouse_name": "LH_Bronze_Data",
# META       "default_lakehouse_workspace_id": "8c76cb1c-cf62-4ebe-b9df-299509803689",
# META       "known_lakehouses": [
# META         {
# META           "id": "edf1817a-d905-4499-83e8-15bb60b7f03e"
# META         },
# META         {
# META           "id": "6d5c070e-4d6b-4904-9b0f-007e3d70dec6"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **Lấy File dữ liệu update mới nhất**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_utc_timestamp, input_file_name, udf
from pyspark.sql.types import IntegerType, DecimalType, DoubleType, TimestampType
from datetime import datetime
from datetime import datetime
import os

# Xử lý dữ liệu phát sinh
base_path = f"Files/SalesData"

# Sử dụng FileSystem API để liệt kê các tệp trong thư mục
files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).listStatus(
    spark._jvm.org.apache.hadoop.fs.Path(base_path)
)

# Chuyển đổi danh sách các tệp từ Java về Python và lấy đường dẫn đầy đủ
files_list = [file.getPath().toString() for file in files]
#print(f"{str(files_list)}")

# Lọc tệp theo thời gian sửa đổi và chọn tệp mới nhất
latest_file = max(files_list, key=lambda x: spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).getFileStatus(spark._jvm.org.apache.hadoop.fs.Path(x)).getModificationTime())
# Đọc tệp mới nhất
df = spark.read.format("csv")\
    .option("header", "true")\
    .load(latest_file)
# Thêm cột LastModifiedDate (giờ hiện tại theo Việt Nam)
df = df.withColumn("LastModifiedDate", from_utc_timestamp(current_timestamp(), "Asia/Ho_Chi_Minh"))

# Chuẩn hóa schema
df = df.select(
    col("ID").cast(IntegerType()),
    col("Order_No"),
    col("Order_Date"),
    col("Order_Day").cast(IntegerType()),
    col("Order_Month").cast(IntegerType()),
    col("Order_Year").cast(IntegerType()),
    col("Ship_Mode"),
    col("Ship_Date"),
    col("Ship_Day").cast(IntegerType()),
    col("Ship_Month").cast(IntegerType()),
    col("Ship_Year").cast(IntegerType()),
    col("Customer_Name"),
    col("Address"),
    col("City"),
    col("State"),
    col("Customer_Type"),
    col("Account_Manager"),
    col("Order_Priority"),
    col("Product_Name"),
    col("Product_Category"),
    col("Product_Container"),
    col("Cost_Price").cast(DecimalType(10, 2)),
    col("Retail_Price").cast(DecimalType(10, 2)),
    col("Profit_Margin").cast(DecimalType(10, 2)),
    col("Profit").cast(DecimalType(10, 2)),
    col("Order_Quantity").cast(IntegerType()),
    col("Sub_Total").cast(DecimalType(10, 2)),
    col("Discount_Percentage").cast(DoubleType()),
    col("Discount_Money").cast(DecimalType(10, 2)),
    col("Order_Total").cast(DecimalType(10, 2)),
    col("Shipping_Cost").cast(DecimalType(10, 2)),
    col("Total").cast(DecimalType(10, 2)),
    col("LastModifiedDate")
)
# Hiển thị kết quả
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Upsert Data**

# CELL ********************

from delta.tables import DeltaTable
# Đọc dữ liệu từ bảng Delta Silver
silver_table_path = "abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/edf1817a-d905-4499-83e8-15bb60b7f03e/Tables/SalesData"
silver_table = DeltaTable.forPath(spark, silver_table_path)

# Điều kiện để thực hiện upsert: khi id đã có trong bảng Silver thì update, nếu chưa có thì insert mới
upsert_condition = "bronze_data.ID = silver_data.ID"

# Thực hiện upsert
silver_table.alias("silver_data").merge(
    df.alias("bronze_data"),
    upsert_condition
).whenMatchedUpdate(
    condition = "bronze_data.LastModifiedDate > silver_data.LastModifiedDate",  # Cập nhật khi dữ liệu mới hơn
    set = {
        "Id": "bronze_data.Id",
        "Order_No": "bronze_data.Order_No",
        "Order_Date": "bronze_data.Order_Date",
        "Order_Day": "bronze_data.Order_Day",
        "Order_Month": "bronze_data.Order_Month",
        "Order_Year": "bronze_data.Order_Year",
        "Ship_Mode": "bronze_data.Ship_Mode",
        "Ship_Date": "bronze_data.Ship_Date",
        "Ship_Day": "bronze_data.Ship_Day",
        "Ship_Month": "bronze_data.Ship_Month",
        "Ship_Year": "bronze_data.Ship_Year",
        "Customer_Name": "bronze_data.Customer_Name",
        "Address": "bronze_data.Address",
        "City": "bronze_data.City",
        "State": "bronze_data.State",
        "Customer_Type": "bronze_data.Customer_Type",
        "Account_Manager": "bronze_data.Account_Manager",
        "Order_Priority": "bronze_data.Order_Priority",
        "Product_Name": "bronze_data.Product_Name",
        "Product_Category": "bronze_data.Product_Category",
        "Product_Container": "bronze_data.Product_Container",
        "Cost_Price": "bronze_data.Cost_Price",
        "Retail_Price": "bronze_data.Retail_Price",
        "Profit": "bronze_data.Profit",
        "Profit_Margin": "bronze_data.Profit_Margin",
        "Order_Quantity": "bronze_data.Order_Quantity",
        "Sub_Total": "bronze_data.Sub_Total",
        "Discount_Percentage": "bronze_data.Discount_Percentage",
        "Discount_Money": "bronze_data.Discount_Money",
        "Order_Total": "bronze_data.Order_Total",
        "Shipping_Cost": "bronze_data.Shipping_Cost",
        "Total": "bronze_data.Total",
        "LastModifiedDate": "bronze_data.LastModifiedDate"
    }
).whenNotMatchedInsert(
    values = {
        "Id": "bronze_data.Id",
        "Order_No": "bronze_data.Order_No",
        "Order_Date": "bronze_data.Order_Date",
        "Order_Day": "bronze_data.Order_Day",
        "Order_Month": "bronze_data.Order_Month",
        "Order_Year": "bronze_data.Order_Year",
        "Ship_Mode": "bronze_data.Ship_Mode",
        "Ship_Date": "bronze_data.Ship_Date",
        "Ship_Day": "bronze_data.Ship_Day",
        "Ship_Month": "bronze_data.Ship_Month",
        "Ship_Year": "bronze_data.Ship_Year",
        "Customer_Name": "bronze_data.Customer_Name",
        "Address": "bronze_data.Address",
        "City": "bronze_data.City",
        "State": "bronze_data.State",
        "Customer_Type": "bronze_data.Customer_Type",
        "Account_Manager": "bronze_data.Account_Manager",
        "Order_Priority": "bronze_data.Order_Priority",
        "Product_Name": "bronze_data.Product_Name",
        "Product_Category": "bronze_data.Product_Category",
        "Product_Container": "bronze_data.Product_Container",
        "Cost_Price": "bronze_data.Cost_Price",
        "Retail_Price": "bronze_data.Retail_Price",
        "Profit_Margin": "bronze_data.Profit_Margin",
        "Profit": "bronze_data.Profit",
        "Order_Quantity": "bronze_data.Order_Quantity",
        "Sub_Total": "bronze_data.Sub_Total",
        "Discount_Percentage": "bronze_data.Discount_Percentage",
        "Discount_Money": "bronze_data.Discount_Money",
        "Order_Total": "bronze_data.Order_Total",
        "Shipping_Cost": "bronze_data.Shipping_Cost",
        "Total": "bronze_data.Total",
        "LastModifiedDate": "bronze_data.LastModifiedDate"
    }
).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
