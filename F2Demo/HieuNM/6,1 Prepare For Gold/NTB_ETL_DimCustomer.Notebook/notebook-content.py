# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f882f219-ac63-4ede-a271-88496b26fa61",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "8c76cb1c-cf62-4ebe-b9df-299509803689",
# META       "known_lakehouses": [
# META         {
# META           "id": "f882f219-ac63-4ede-a271-88496b26fa61"
# META         },
# META         {
# META           "id": "d2de7937-3916-40ac-bfb4-00a94530317f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import col, row_number, coalesce, max, lit
from pyspark.sql.window import Window
from delta.tables import *

# Tạo bảng DimCustomer nếu chưa tồn tại
DeltaTable.createIfNotExists(spark) \
    .tableName("DimCustomer") \
    .addColumn("Customer_ID", LongType()) \
    .addColumn("Customer_Name", StringType()) \
    .addColumn("Address", StringType()) \
    .addColumn("City", StringType()) \
    .addColumn("State", StringType()) \
    .addColumn("Customer_Type", StringType()) \
    .addColumn("Account_Manager", StringType()) \
    .execute()

# Đọc dữ liệu phát sinh từ Silver Layer
df = spark.read.format("delta").load(
    "abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/d2de7937-3916-40ac-bfb4-00a94530317f/Tables/SalesData"
)

# Lọc và giữ các cột cần thiết, loại bỏ trùng lặp
dfDimCustomer = df.select(
    col("Customer_Name"),
    col("Address"),
    col("City"),
    col("State"),
    col("Customer_Type"),
    col("Account_Manager")
).dropna().dropDuplicates(["Customer_Name"]) \
 .filter(col("Customer_Name").isNotNull())

# Tạo ID mới
windowSpec = Window().orderBy("Customer_Name")
dfDimCustomer = dfDimCustomer.withColumn(
    "Customer_ID",
    row_number().over(windowSpec)
)

# Merge vào bảng DimCustomer
deltaTable = DeltaTable.forName(spark, "DimCustomer")

deltaTable.alias("gold") \
    .merge(
        dfDimCustomer.alias("updates"),
        "gold.Customer_Name <=> updates.Customer_Name AND gold.Address <=> updates.Address"
    ) \
    .whenMatchedUpdate(set={
        "Address": "updates.Address",
        "City": "updates.City",
        "State": "updates.State",
        "Customer_Type": "updates.Customer_Type",
        "Account_Manager": "updates.Account_Manager"
    }) \
    .whenNotMatchedInsert(values={
        "Customer_ID": "updates.Customer_ID",
        "Customer_Name": "updates.Customer_Name",
        "Address": "updates.Address",
        "City": "updates.City",
        "State": "updates.State",
        "Customer_Type": "updates.Customer_Type",
        "Account_Manager": "updates.Account_Manager"
    }) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
