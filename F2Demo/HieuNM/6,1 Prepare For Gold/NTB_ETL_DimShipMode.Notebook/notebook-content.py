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

# Tạo bảng DimShipMode nếu chưa tồn tại
DeltaTable.createIfNotExists(spark) \
    .tableName("DimShipMode") \
    .addColumn("ShipMode_ID", LongType()) \
    .addColumn("Ship_Mode", StringType()) \
    .execute()

# Bước 1: Đọc dữ liệu phát sinh từ Silver Layer
df = spark.read.format("delta").load(
    "abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/d2de7937-3916-40ac-bfb4-00a94530317f/Tables/SalesData"
)

# Bước 2: Lọc và giữ cột Ship_Mode, loại bỏ trùng lặp
dfDimShipMode = df.select(
    col("Ship_Mode")
).dropna().dropDuplicates(["Ship_Mode"]) \
 .filter(col("Ship_Mode").isNotNull())

# Bước 3: Tạo ID mới
windowSpec = Window().orderBy("Ship_Mode")
dfDimShipMode = dfDimShipMode.withColumn(
    "ShipMode_ID",
    row_number().over(windowSpec)
)

# Bước 4: Merge vào bảng DimShipMode
deltaTable = DeltaTable.forName(spark, "DimShipMode")

print(f"Number of records to upsert into DimShipMode: {dfDimShipMode.count()}")

deltaTable.alias("gold") \
    .merge(
        dfDimShipMode.alias("updates"),
        "gold.Ship_Mode <=> updates.Ship_Mode"
    ) \
    .whenNotMatchedInsert(values={
        "ShipMode_ID": "updates.ShipMode_ID",
        "Ship_Mode": "updates.Ship_Mode"
    }) \
    .execute()
print(f"ID MAX: {MAXShipModeID}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
