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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, explode_outer, explode

def flatten(df):
    flat_cols = []
    explode_cols = []
    
    for field in df.schema.fields:
        dtype = field.dataType
        name = field.name
        
        if isinstance(dtype, StructType):
            for subfield in dtype.fields:
                flat_cols.append(col(f"{name}.{subfield.name}").alias(f"{name}_{subfield.name}"))
        elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
            explode_cols.append(name)
        else:
            flat_cols.append(col(name))
    
    if explode_cols:
        df = df.withColumn(explode_cols[0], explode_outer(explode_cols[0]))
        return flatten(df)
    else:
        return df.select(flat_cols)

# Dùng với JSON của bạn
df = spark.read.json("Files/API/2025/08/20")
df_exploded = df.select(explode("data.items").alias("item")).select("item.*")
df_flattened = flatten(df_exploded)

display(df_flattened)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
