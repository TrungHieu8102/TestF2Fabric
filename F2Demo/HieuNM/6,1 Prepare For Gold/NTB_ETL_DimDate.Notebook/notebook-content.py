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
from pyspark.sql.functions import col, row_number, coalesce, max, lit, to_date, concat_ws
from pyspark.sql.window import Window
from delta.tables import *

# Tạo bảng DimDate nếu chưa tồn tại
DeltaTable.createIfNotExists(spark) \
    .tableName("DimDate") \
    .addColumn("Date_ID", LongType()) \
    .addColumn("Day", IntegerType()) \
    .addColumn("Month", IntegerType()) \
    .addColumn("Year", IntegerType()) \
    .execute()

# Đọc dữ liệu phát sinh từ Silver Layer
df = spark.read.format("delta").load(
    "abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/d2de7937-3916-40ac-bfb4-00a94530317f/Tables/SalesData"
)

# Lọc và giữ các cột ngày tháng duy nhất từ Order_Date và Ship_Date
dfOrderDate = df.select(
    col("Order_Day").alias("Day"),
    col("Order_Month").alias("Month"),
    col("Order_Year").alias("Year")
)

dfShipDate = df.select(
    col("Ship_Day").alias("Day"),
    col("Ship_Month").alias("Month"),
    col("Ship_Year").alias("Year")
)

# Kết hợp và loại bỏ trùng lặp
dfDimDate = dfOrderDate.union(dfShipDate).dropna().dropDuplicates(["Day", "Month", "Year"]) \
    .filter(
        (col("Day").between(1, 31)) &
        (col("Month").between(1, 12)) &
        (col("Year").isNotNull())
    )

# Tạo ID mới
windowSpec = Window().orderBy("Year")
dfDimDate = dfDimDate.withColumn(
    "Date_ID",
    row_number().over(windowSpec)
)

# Merge vào bảng DimDate
deltaTable = DeltaTable.forName(spark, "DimDate")

print(f"Number of records to upsert into DimDate: {dfDimDate.count()}")

deltaTable.alias("gold") \
    .merge(
        dfDimDate.alias("updates"),
        "gold.Day <=> updates.Day AND gold.Month <=> updates.Month AND gold.Year <=> updates.Year"
    ) \
    .whenNotMatchedInsert(values={
        "Date_ID": "updates.Date_ID",
        "Day": "updates.Day",
        "Month": "updates.Month",
        "Year": "updates.Year"
    }) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
