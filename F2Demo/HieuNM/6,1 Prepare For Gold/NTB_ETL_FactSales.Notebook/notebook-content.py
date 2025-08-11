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
from pyspark.sql.functions import col, to_date, concat_ws
from delta.tables import *

# Tạo bảng FactSales nếu chưa tồn tại
DeltaTable.createIfNotExists(spark) \
    .tableName("FactSales") \
    .addColumn("Order_ID", LongType()) \
    .addColumn("Date_ID", LongType()) \
    .addColumn("ShipDate_ID", LongType()) \
    .addColumn("Customer_ID", LongType()) \
    .addColumn("Product_ID", LongType()) \
    .addColumn("ShipMode_ID", LongType()) \
    .addColumn("Order_Quantity", IntegerType()) \
    .addColumn("Profit_Margin", DoubleType()) \
    .addColumn("Sub_Total", DoubleType()) \
    .addColumn("Discount_Percentage", DoubleType()) \
    .addColumn("Discount_Money", DoubleType()) \
    .addColumn("Order_Total", DoubleType()) \
    .addColumn("Shipping_Cost", DoubleType()) \
    .addColumn("Total", DoubleType()) \
    .execute()

# Bước 1: Đọc dữ liệu phát sinh từ Silver Layer
df = spark.read.format("delta").load(
    "abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/d2de7937-3916-40ac-bfb4-00a94530317f/Tables/SalesData"
)

# Bước 2: Đọc các bảng Dim để ánh xạ khóa
dfDimOrder = spark.read.table("DimOrder").select("Order_ID", "Order_No")
dfDimDate = spark.read.table("DimDate").select("Date_ID", "Day", "Month", "Year")
dfDimShipMode = spark.read.table("DimShipMode").select("ShipMode_ID", "Ship_Mode")
dfDimCustomer = spark.read.table("DimCustomer").select("Customer_ID", "Customer_Name","Address")
dfDimProduct = spark.read.table("DimProduct").select("Product_ID", "Product_Name")

# Bước 3: Tham gia dữ liệu để lấy khóa chính
dfFact = df.select(
    col("Order_No"),
    col("Order_Day"),
    col("Order_Month"),
    col("Order_Year"),
    col("Ship_Day"),
    col("Ship_Month"),
    col("Ship_Year"),
    col("Customer_Name"),
    col("Address"),
    col("Product_Name"),
    col("Ship_Mode"),
    col("Order_Quantity"),
    col("Profit_Margin"),
    col("Sub_Total"),
    col("Discount_Percentage"),
    col("Discount_Money"),
    col("Order_Total"),
    col("Shipping_Cost"),
    col("Total")
).join(
    dfDimOrder,
    df["Order_No"] == dfDimOrder["Order_No"],
    "inner"
).join(
    dfDimDate.alias("order_date"),
    (df["Order_Day"] == col("order_date.Day")) &
    (df["Order_Month"] == col("order_date.Month")) &
    (df["Order_Year"] == col("order_date.Year")),
    "inner"
).join(
    dfDimDate.alias("ship_date"),
    (df["Ship_Day"] == col("ship_date.Day")) &
    (df["Ship_Month"] == col("ship_date.Month")) &
    (df["Ship_Year"] == col("ship_date.Year")),
    "inner"
).join(
    dfDimCustomer,
    (df["Customer_Name"] == dfDimCustomer["Customer_Name"]) &  
    (df["Address"] == dfDimCustomer["Address"]),
    "inner"
).join(
    dfDimProduct,
    df["Product_Name"] == dfDimProduct["Product_Name"],
    "inner"
).join(
    dfDimShipMode,
    df["Ship_Mode"] == dfDimShipMode["Ship_Mode"],
    "inner"
).select(
    dfDimOrder["Order_ID"],
    col("order_date.Date_ID").alias("Date_ID"),
    col("ship_date.Date_ID").alias("ShipDate_ID"),
    dfDimCustomer["Customer_ID"],
    dfDimProduct["Product_ID"],
    dfDimShipMode["ShipMode_ID"],
    col("Order_Quantity"),
    col("Sub_Total"),
    col("Profit_Margin"),
    col("Discount_Percentage"),
    col("Discount_Money"),
    col("Order_Total"),
    col("Shipping_Cost"),
    col("Total")
).dropna().filter(
    (col("Order_Quantity") >= 0) &
    (col("Sub_Total") >= 0) &
    (col("Order_Total") >= 0)
)

# Bước 4: Merge vào bảng FactSales
deltaTable = DeltaTable.forName(spark, "FactSales")

print(f"Number of records to upsert into FactSales: {dfFact.count()}")

deltaTable.alias("gold") \
    .merge(
        dfFact.alias("updates"),
        "gold.Order_ID = updates.Order_ID AND gold.Date_ID = updates.Date_ID AND gold.ShipDate_ID = updates.ShipDate_ID AND gold.Customer_ID = updates.Customer_ID AND gold.Product_ID = updates.Product_ID AND gold.ShipMode_ID = updates.ShipMode_ID"
    ) \
    .whenNotMatchedInsert(values={
        "Order_ID": "updates.Order_ID",
        "Date_ID": "updates.Date_ID",
        "ShipDate_ID": "updates.ShipDate_ID",
        "Customer_ID": "updates.Customer_ID",
        "Product_ID": "updates.Product_ID",
        "ShipMode_ID": "updates.ShipMode_ID",
        "Order_Quantity": "updates.Order_Quantity",
        "Profit_Margin": "updates.Profit_Margin",
        "Sub_Total": "updates.Sub_Total",
        "Discount_Percentage": "updates.Discount_Percentage",
        "Discount_Money": "updates.Discount_Money",
        "Order_Total": "updates.Order_Total",
        "Shipping_Cost": "updates.Shipping_Cost",
        "Total": "updates.Total"
    }) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
