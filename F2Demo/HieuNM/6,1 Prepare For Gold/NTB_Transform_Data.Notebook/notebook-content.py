# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d2de7937-3916-40ac-bfb4-00a94530317f",
# META       "default_lakehouse_name": "Silver_LH",
# META       "default_lakehouse_workspace_id": "8c76cb1c-cf62-4ebe-b9df-299509803689",
# META       "known_lakehouses": [
# META         {
# META           "id": "d2de7937-3916-40ac-bfb4-00a94530317f"
# META         },
# META         {
# META           "id": "14c8bfa1-1b99-48df-a367-1529dc3b8fa6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import*
from pyspark.sql.functions import from_utc_timestamp
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import to_date, col,date_format
from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name
from pyspark.sql import functions as F
from pyspark.sql.functions import row_number, lit
from pyspark.sql.window import Window

# Đọc file dữ liệu
df = spark.read.format("csv")\
    .option("header", "true")\
    .load("abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/14c8bfa1-1b99-48df-a367-1529dc3b8fa6/Files/data.csv")

# Bỏ dòng nếu các cột quan trọng bị NULL
df = df.dropna(subset=[
    'Order No', 'Order Date', 'Customer Name',
    'Product Name', 'Product Category', 'Order Quantity',
    'Address', 'City', 'State', 'Ship Date'
])

# Thay thế NULL bằng giá trị mặc định
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

# chuẩn hóa về dạng số thập phân các cột liên quan giá tiền
money_columns = ["Cost Price", "Profit Margin", "Retail Price", "Sub Total", "Discount $", "Order Total", "Shipping Cost", "Total"]
for col_name in money_columns:
    df = df.withColumn(col_name,regexp_replace(col(col_name), "[$,]", "").cast(DecimalType(10,2)))
df = df.withColumn("Discount %", (regexp_replace(col("Discount %"), "%", "").cast("float") / 100))

# chuẩn hóa về dạng Date các cột liên quan đến ngày tháng năm
from pyspark.sql.functions import to_date, dayofmonth, month, year, col

# Chuyển đổi định dạng ngày
df = df.withColumn("Order Date", to_date(col("Order Date"), "dd-MM-yyyy")) \
       .withColumn("Ship Date", to_date(col("Ship Date"), "dd-MM-yyyy"))

# Thêm cột day, month, year cho Order Date
df = df.withColumn("Order_Day", dayofmonth(col("Order Date"))) \
       .withColumn("Order_Month", month(col("Order Date"))) \
       .withColumn("Order_Year", year(col("Order Date")))

# Thêm cột day, month, year cho Ship Date
df = df.withColumn("Ship_Day", dayofmonth(col("Ship Date"))) \
       .withColumn("Ship_Month", month(col("Ship Date"))) \
       .withColumn("Ship_Year", year(col("Ship Date")))

# Chuyển Order_Date và Ship_Date về dạng chuỗi "MM-dd-yyyy"
df = df.withColumn("Order Date", date_format(col("Order Date"), "MM-dd-yyyy")) \
       .withColumn("Ship Date", date_format(col("Ship Date"), "MM-dd-yyyy"))

# chuẩn hóa tên cột phục vụ về sau
df = df.withColumnRenamed("Discount %", "Discount_Percentage")
df = df.withColumnRenamed("Discount $", "Discount_Money")
for col_name in df.columns:
    new_col_name = col_name.replace(" ", "_")
    df = df.withColumnRenamed(col_name, new_col_name)
df = df.withColumn("Order_Quantity", col("Order_Quantity").cast("integer"))

# Thêm cột 'LastModifiedDate' trong df với thời gian hiện tại
df = df.withColumn("LastModifiedDate", from_utc_timestamp(current_timestamp(), "Asia/Ho_Chi_Minh"))

# Tạo ID tuần tự bắt đầu từ 1
window_spec = Window.orderBy(lit(1))
df = df.withColumn("ID", row_number().over(window_spec))
cols = df.columns

# Danh sách cột đưa lên đầu
priority_cols = ["ID", "Order_No", "Order_Date", "Order_Day", "Order_Month", "Order_Year",
                 "Ship_Mode", "Ship_Date", "Ship_Day", "Ship_Month", "Ship_Year"]

# Tạo danh sách các cột còn lại mà không có trong priority_cols
remaining_cols = [col for col in df.columns if col not in priority_cols]

# Gộp lại: các cột ưu tiên lên trước, phần còn lại giữ nguyên thứ tự
new_order = priority_cols + remaining_cols

df = df.select(*new_order)  # chọn lại thứ tự cột

display(df)
# Load dữ liệu 
df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/d2de7937-3916-40ac-bfb4-00a94530317f/Tables/SalesData")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
