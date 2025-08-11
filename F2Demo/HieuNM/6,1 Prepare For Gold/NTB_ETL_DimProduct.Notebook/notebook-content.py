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

# Tạo bảng DimProduct nếu chưa tồn tại
DeltaTable.createIfNotExists(spark) \
    .tableName("DimProduct") \
    .addColumn("Product_ID", LongType()) \
    .addColumn("Product_Name", StringType()) \
    .addColumn("Product_Category", StringType()) \
    .addColumn("Product_Container", StringType()) \
    .addColumn("Cost_Price", DoubleType()) \
    .addColumn("Retail_Price", DoubleType()) \
    .execute()

# Bước 1: Đọc dữ liệu phát sinh từ Silver Layer
df = spark.read.format("delta").load(
    "abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/d2de7937-3916-40ac-bfb4-00a94530317f/Tables/SalesData"
)

# Bước 2: Lọc và giữ các cột cần thiết, loại bỏ trùng lặp
dfDimProduct = df.select(
    col("Product_Name"),
    col("Product_Category"),
    col("Product_Container"),
    col("Cost_Price"),
    col("Retail_Price")
).dropna().dropDuplicates(["Product_Name"]) \
 .filter(
     (col("Product_Name").isNotNull()) &
     (col("Cost_Price") >= 0) &
     (col("Retail_Price") >= 0)
 )

# Tạo ID mới
windowSpec = Window().orderBy("Product_Name")
dfDimProduct = dfDimProduct.withColumn(
    "Product_ID",
    row_number().over(windowSpec)
)

# Merge vào bảng DimProduct
deltaTable = DeltaTable.forName(spark, "DimProduct")

deltaTable.alias("gold") \
    .merge(
        dfDimProduct.alias("updates"),
        "gold.Product_Name <=> updates.Product_Name"
    ) \
    .whenMatchedUpdate(set={
        "Product_Category": "updates.Product_Category",
        "Product_Container": "updates.Product_Container",
        "Cost_Price": "updates.Cost_Price",
        "Retail_Price": "updates.Retail_Price"
    }) \
    .whenNotMatchedInsert(values={
        "Product_ID": "updates.Product_ID",
        "Product_Name": "updates.Product_Name",
        "Product_Category": "updates.Product_Category",
        "Product_Container": "updates.Product_Container",
        "Cost_Price": "updates.Cost_Price",
        "Retail_Price": "updates.Retail_Price"
    }) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
