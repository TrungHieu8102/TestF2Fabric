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
# META           "id": "6d5c070e-4d6b-4904-9b0f-007e3d70dec6"
# META         },
# META         {
# META           "id": "edf1817a-d905-4499-83e8-15bb60b7f03e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# === Đọc dữ liệu từ file CSV ===
df = spark.read.format("csv")\
    .option("header", "true")\
    .load("abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/6d5c070e-4d6b-4904-9b0f-007e3d70dec6/Files/SalesData/data.csv")

# === Bỏ các dòng có NULL ở cột quan trọng ===
df = df.dropna(subset=[
    'Order No', 'Order Date', 'Customer Name',
    'Product Name', 'Product Category', 'Order Quantity',
    'Address', 'City', 'State', 'Ship Date'
])

# === Thay NULL bằng giá trị mặc định ===
df = df.fillna({
    'Customer Type': 'Unknown',
    'Account Manager': 'Unassigned',
    'Order Priority': 'Normal',
    'Product Container': 'Standard',
    'Ship Mode': 'Standard',
    'Cost Price': 0,
    'Retail Price': 0,
    'Profit Margin': 0,
    'Sub Total': 0,
    'Discount %': 0,
    'Discount $': 0,
    'Order Total': 0,
    'Shipping Cost': 0,
    'Total': 0
})

# === Chuẩn hóa các cột tiền tệ về Decimal(10,2) ===
money_columns = ["Cost Price", "Profit Margin", "Retail Price", "Sub Total",
                 "Discount $", "Order Total", "Shipping Cost", "Total"]

for col_name in money_columns:
    df = df.withColumn(col_name, regexp_replace(col(col_name), "[$,]", "").cast(DecimalType(10, 2)))

# === Chuẩn hóa Discount % ===
df = df.withColumn("Discount %", regexp_replace(col("Discount %"), "%", "").cast("float") / 100)

# === Ép kiểu Order Quantity ===
df = df.withColumn("Order Quantity", col("Order Quantity").cast("integer"))

# === Tính toán các trường tài chính ===
df = df.withColumn("Sub Total", (col("Retail Price") * col("Order Quantity")).cast(DecimalType(10, 2))) \
       .withColumn("Discount $", (col("Sub Total") * col("Discount %")).cast(DecimalType(10, 2))) \
       .withColumn("Order Total", (col("Sub Total") - col("Discount $")).cast(DecimalType(10, 2))) \
       .withColumn("Total", (col("Order Total") + col("Shipping Cost")).cast(DecimalType(10, 2))) \
       .withColumn("Profit", ((col("Retail Price") - col("Cost Price"))*col("Order Quantity")).cast(DecimalType(10, 2))) \
       .withColumn("Profit Margin",
           when(col("Retail Price") != 0,
                ((col("Profit") / col("Sub Total")) * 100).cast(DecimalType(10, 2)))
           .otherwise(lit(0).cast(DecimalType(10, 2))))

# === Chuẩn hóa ngày tháng ===
df = df.withColumn("Order Date", to_date(col("Order Date"), "dd-MM-yyyy")) \
       .withColumn("Ship Date", to_date(col("Ship Date"), "dd-MM-yyyy"))

df = df.withColumn("Order_Day", dayofmonth(col("Order Date"))) \
       .withColumn("Order_Month", month(col("Order Date"))) \
       .withColumn("Order_Year", year(col("Order Date")))

df = df.withColumn("Ship_Day", dayofmonth(col("Ship Date"))) \
       .withColumn("Ship_Month", month(col("Ship Date"))) \
       .withColumn("Ship_Year", year(col("Ship Date")))

# Format lại nếu cần hiển thị ngày ở dạng chuỗi
df = df.withColumn("Order Date", date_format(col("Order Date"), "MM-dd-yyyy")) \
       .withColumn("Ship Date", date_format(col("Ship Date"), "MM-dd-yyyy"))

# === Đổi tên cột: bỏ dấu cách và ký tự đặc biệt ===
df = df.withColumnRenamed("Discount %", "Discount_Percentage") \
       .withColumnRenamed("Discount $", "Discount_Money")

for col_name in df.columns:
    df = df.withColumnRenamed(col_name, col_name.replace(" ", "_"))

# === Thêm cột thời gian cập nhật ===
df = df.withColumn("LastModifiedDate", from_utc_timestamp(current_timestamp(), "Asia/Ho_Chi_Minh"))

# === Thêm ID tăng dần ===
window_spec = Window.orderBy(lit(1))
df = df.withColumn("ID", row_number().over(window_spec))

# === Đưa các cột quan trọng lên đầu ===
priority_cols = ["ID", "Order_No", "Order_Date", "Order_Day", "Order_Month", "Order_Year",
                 "Ship_Mode", "Ship_Date", "Ship_Day", "Ship_Month", "Ship_Year"]

remaining_cols = [col for col in df.columns if col not in priority_cols]
df = df.select(priority_cols + remaining_cols)

# === Hiển thị nếu trong notebook ===
display(df)

# === Ghi dữ liệu ra Delta Lake ===
df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/edf1817a-d905-4499-83e8-15bb60b7f03e/Tables/SalesData")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
