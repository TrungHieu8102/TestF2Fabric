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

# === Import thư viện ===
from pyspark.sql.functions import concat_ws, sha2
import pyspark.sql.functions as F
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import mlflow
import mlflow.sklearn
import matplotlib.pyplot as plt

# === Bước 1: Đọc dữ liệu Silver và tạo Customer_ID duy nhất ===
df_silver = spark.read.format("delta").load(
    "abfss://8c76cb1c-cf62-4ebe-b9df-299509803689@onelake.dfs.fabric.microsoft.com/d2de7937-3916-40ac-bfb4-00a94530317f/Tables/SalesData"
)

df_silver = df_silver.select(
    "Customer_Name", "Address", "City", "State",
    "Order_Quantity", "Order_Total", "Discount_Money", "Customer_Type"
)

df_silver = df_silver.withColumn(
    "Customer_ID",
    sha2(concat_ws("_", "Customer_Name", "Address", "State", "City"), 256)
)

# === Bước 2: Trích xuất đặc trưng khách hàng ===
df_features = df_silver.groupBy("Customer_ID").agg(
    F.avg("Order_Quantity").alias("Avg_Order_Quantity"),
    F.avg("Order_Total").alias("Avg_Order_Total"),
    F.avg("Discount_Money").alias("Avg_Discount")
).toPandas()

# === Bước 3: Tiền xử lý dữ liệu ===
X = df_features[["Avg_Order_Quantity", "Avg_Order_Total", "Avg_Discount"]]
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# === Bước 4: Tìm số cụm tối ưu bằng Elbow + Silhouette ===
inertias = []
silhouette_scores = []
k_values = range(2, 11)

for k in k_values:
    kmeans = KMeans(n_clusters=k, random_state=42)
    kmeans.fit(X_scaled)
    inertias.append(kmeans.inertia_)
    
    labels = kmeans.predict(X_scaled)
    silhouette_scores.append(silhouette_score(X_scaled, labels))

# === Vẽ biểu đồ Elbow và Silhouette ===
plt.figure(figsize=(12, 5))

plt.subplot(1, 2, 1)
plt.plot(k_values, inertias, marker='o')
plt.title("Elbow Method - Inertia")
plt.xlabel("Số cụm k")
plt.ylabel("Inertia")

plt.subplot(1, 2, 2)
plt.plot(k_values, silhouette_scores, marker='o', color='orange')
plt.title("Silhouette Score")
plt.xlabel("Số cụm k")
plt.ylabel("Silhouette Score")

plt.show()

# === Bước 5: Chọn số cụm tối ưu ===
best_k = k_values[silhouette_scores.index(max(silhouette_scores))]
print(f"Số cụm tối ưu được chọn: {best_k}")

# === Bước 6: Huấn luyện mô hình KMeans ===
kmeans_final = KMeans(n_clusters=best_k, random_state=42)
kmeans_final.fit(X_scaled)

df_features["Cluster_Label"] = kmeans_final.predict(X_scaled)

# === Bước 7: Log mô hình với MLflow ===
with mlflow.start_run():
    mlflow.sklearn.log_model(kmeans_final, "kmeans_model")
    mlflow.log_param("n_clusters", best_k)
    mlflow.log_metric("silhouette_score", max(silhouette_scores))

# === Bước 8: Lưu kết quả phân cụm vào tầng Gold cùng thông tin và đặc trưng ===

# Giữ các cột đặc trưng + nhãn cụm từ pandas DataFrame
df_clusters_pd = df_features[[
    "Customer_ID", "Avg_Order_Quantity", "Avg_Order_Total", "Avg_Discount", "Cluster_Label"
]]

# Chuyển sang Spark DataFrame
df_clusters_spark = spark.createDataFrame(df_clusters_pd)

# Lấy thông tin khách hàng gốc từ df_silver
df_customer_info = df_silver.select(
    "Customer_ID", "Customer_Name", "Address", "City", "State", "Customer_Type"
).dropDuplicates(["Customer_ID"])

# Join để kết hợp đầy đủ thông tin
df_customer_clusters = df_customer_info.join(df_clusters_spark, on="Customer_ID", how="inner")

# Lưu bảng phân cụm đầy đủ vào tầng Gold
df_customer_clusters.write.format("delta").mode("overwrite").save(
    "abfss://WS_Finance_Medallion@onelake.dfs.fabric.microsoft.com/LH_Golden_Data.Lakehouse/Tables/CustomerClusters"
)

print("Phân cụm khách hàng hoàn tất và lưu vào tầng Gold cùng thông tin và đặc trưng.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
