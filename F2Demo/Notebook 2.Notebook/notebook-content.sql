-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "4b57cc09-e0a2-4ffd-80a0-4520f65d78f2",
-- META       "default_lakehouse_name": "LKH_Bronze_Data",
-- META       "default_lakehouse_workspace_id": "8c76cb1c-cf62-4ebe-b9df-299509803689",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "4b57cc09-e0a2-4ffd-80a0-4520f65d78f2"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "a0f5f01e-2f78-893f-4b83-3177557663ee",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "a0f5f01e-2f78-893f-4b83-3177557663ee",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

UPDATE dbo.WaterMark
SET LastModifiedDate = '1900-01-01'

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Bước 1: Tạo DataFrame mẫu
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Spark session thường đã được khởi tạo sẵn trong notebook
data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie")
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

df = spark.createDataFrame(data, schema)

# Bước 2: Ghi vào bảng trong Lakehouse
# Lakehouse mặc định được mount ở đường dẫn /lakehouse/default
# Nếu LKH_Bronze_Data là lakehouse được gắn vào notebook, dùng path sau:
df.write.format("delta").mode("overwrite").saveAsTable("Test")


-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark",
-- META   "frozen": true,
-- META   "editable": false
-- META }

-- CELL ********************

spark.sql("SELECT * FROM Test").show()


-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark",
-- META   "frozen": true,
-- META   "editable": false
-- META }
