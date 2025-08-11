# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4b57cc09-e0a2-4ffd-80a0-4520f65d78f2",
# META       "default_lakehouse_name": "LKH_Bronze_Data",
# META       "default_lakehouse_workspace_id": "8c76cb1c-cf62-4ebe-b9df-299509803689",
# META       "known_lakehouses": [
# META         {
# META           "id": "4b57cc09-e0a2-4ffd-80a0-4520f65d78f2"
# META         },
# META         {
# META           "id": "ecfbd831-77f5-4131-970d-fdd4541018c5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.parquet("Files/Sharepoint/ChamCong/2025/05/22/11223fa8-2b5a-4433-9f53-2bca5a6e08fa/QuảnLýChấmCông.parquet")
# df now is a Spark DataFrame containing parquet data from "Files/Sharepoint/ChamCong/2025/05/22/11223fa8-2b5a-4433-9f53-2bca5a6e08fa/QuảnLýChấmCông.parquet".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if the column exists
if 'ModifiedById' in df.columns:
    # Count distinct values in the 'ModifiedById' column
    distinct_count = df.select("ModifiedById").distinct().count()
    print(f"Distinct count in ModifiedById column: {distinct_count}")
else:
    print("Column ModifiedById does not exist in the loaded data.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

occurrence_count = df.groupBy("ModifiedById").count()
occurrence_count.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# Calculate the sum of 'TransportationAllowance' for each 'EmployeeId'
sum_df = df.groupBy("EmployeeId").agg(sum("TransportationAllowance").alias("TotalTransportationAllowance"))

# Join the calculated sum back to the original DataFrame
result_df = df.join(sum_df, on="EmployeeId", how="left")

# Show the result
display(result_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Join the calculated sum back to the original DataFrame
result_df_2 = df.join(sum_df, on="EmployeeId", how="inner")

# Show the result
display(result_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
window_spec = Window.partitionBy("EmployeeId")

# Use withColumn to add the new column 'TotalTransportationAllowance'
result_df = df.withColumn("TotalTransportationAllowance", _sum("TransportationAllowance").over(window_spec))

# Show the result
display(result_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df_2.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
