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

# Tạo bảng DimOrder nếu chưa tồn tại
DeltaTable.createIfNotExists(spark) \
    .tableName("DimOrder") \
    .addColumn("Order_Priority", StringType()) \
    .addColumn("Order_No", StringType()) \
    .addColumn("Order_ID", LongType()) \
    .execute()

# Bước 1: Đọc dữ liệu từ Silver Layer
df = spark.read.format("delta").load(
    "abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/d2de7937-3916-40ac-bfb4-00a94530317f/Tables/SalesData"
)

# Bước 2: Lọc và giữ các cột cần thiết, loại bỏ trùng lặp
dfDimOrder = df.select(
    col("Order_Priority"),
    col("Order_No")
).dropna().dropDuplicates(["Order_Priority", "Order_No"])


# Bước 3: Tạo ID mới với row_number()
windowSpec = Window.orderBy("Order_No")  # Sắp xếp để đảm bảo thứ tự nhất quán
dfDimOrder = dfDimOrder.withColumn(
    "Order_ID",
    row_number().over(windowSpec) 
)


# Bước 4: Merge vào bảng DimOrder
deltaTable = DeltaTable.forName(spark, "dimorder")

print(f"Number of records to upsert into DimOrder: {dfDimOrder.count()}")

deltaTable.alias("gold") \
    .merge(
        dfDimOrder.alias("updates"),
        "gold.Order_No <=> updates.Order_No AND gold.Order_Priority <=> updates.Order_Priority"
    ) \
    .whenMatchedUpdate(set={
        "Order_Priority": "updates.Order_Priority"
    }) \
    .whenNotMatchedInsert(values={
        "Order_ID": "updates.Order_ID",
        "Order_No": "updates.Order_No",
        "Order_Priority": "updates.Order_Priority"
    }) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
