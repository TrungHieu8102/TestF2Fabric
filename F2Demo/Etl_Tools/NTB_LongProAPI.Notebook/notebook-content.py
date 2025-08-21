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

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Fabric Notebook: PySpark – Fetch nội bộ REST API (Bearer) và ghi JSON vào Lakehouse
# ==================================================================================
# CHÚ Ý KẾT NỐI MẠNG:
# - Notebook chạy trên Fabric cloud. Muốn gọi được API nội bộ (192.168.x.x),
#   bạn cần đảm bảo Spark có đường mạng tới API (VPN/VNet/tunnel hoặc public hóa tạm thời).
# - Nếu chưa có đường, lệnh gọi HTTP sẽ timeout. Code dưới đây đã tối ưu retry, nhưng
#   không thay thế được kết nối mạng.

# COMMAND ----------
# 1) Cấu hình cơ bản
from datetime import datetime
import json

# URL API nội bộ
API_URL = "http://192.168.21.49:8083/api/dstb/filter/0/10"

# ====== BẢO MẬT TOKEN ======
# Cách khuyên dùng (nếu bạn đã tạo Fabric workspace secret):
#   from notebookutils import mssparkutils
#   BEARER_TOKEN = mssparkutils.credentials.getSecret("<secretScope>", "<secretName>")
# Hoặc: dán token tạm thời vào biến dưới (chỉ dùng cho thử nghiệm):
BEARER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiOWYyOWQ3ZTQtM2FiZi00M2NmLTk0MGYtYjUzMWZmYjc4ODQ0IiwidG9rZW5faW5zdGFuY2VfaWQiOiJRTEpNQ05QYkVDcE9zM05yV0s0Vzg4RHpOdWE0VVlraFhPSzZYY20iLCJqdGkiOiIwZWQyYzgyYS00OGQ1LTRlMWItYThjMi04OTBjZDZiMTRmNDciLCJwcm4iOiJkZTM5MTRhZi0xZmJkLTRiMjYtYmQ1MS1mNWJlNjhjNTkxZjMiLCJuYmYiOjE3NTUyNDc3MDUsImV4cCI6MTc1NTI1MTMwNSwiaWF0IjoxNzU1MjQ3NzA1LCJpc3MiOiJodHRwczovL2FwaS1iY3F0LnZpbmNvbS5jb20udm4iLCJhdWQiOiJodHRwczovL2FwaS1iY3F0LnZpbmNvbS5jb20udm4ifQ.EM4QucNoR3tHR5q_E0YCX9RHdxNFWLYPr8cpMGcRjCU"

# Lakehouse đích (đã attach trong UI)
LAKEHOUSE = "LKH_Bronze_Data"   # đổi nếu tên khác
BASE_DIR = f"/lakehouse/{LAKEHOUSE}/Files/raw/api_dstb"
TIMESTAMP = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

# Tuỳ chọn: proxy/SSL cho môi trường doanh nghiệp (bỏ comment nếu cần)
HTTP_KWARGS = {
    # "proxies": {"http": "http://<proxy>:<port>", "https": "http://<proxy>:<port>"},
    # "verify": False,   # KHÔNG khuyến nghị. Chỉ dùng khi môi trường có chứng thư tự ký.
    "timeout": 60,
}

# COMMAND ----------
# 2) Hàm gọi API với retry/backoff
import time
import requests
from requests.adapters import HTTPAdapter, Retry

session = requests.Session()
retries = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
session.mount("http://", HTTPAdapter(max_retries=retries))
session.mount("https://", HTTPAdapter(max_retries=retries))

HEADERS = {"Authorization": f"Bearer {BEARER_TOKEN}", "Accept": "application/json"}

def fetch_json(url: str):
    resp = session.get(url, headers=HEADERS, **HTTP_KWARGS)
    # Nếu muốn POST: session.post(url, headers=HEADERS, json={...}, **HTTP_KWARGS)
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception as ex:
        raise Exception(f"Response không phải JSON hợp lệ: {ex}. Trích: {resp.text[:500]}")

# COMMAND ----------
# 3) Gọi API & lưu RAW JSON 1 file (giữ nguyên cấu trúc trả về)
from notebookutils import mssparkutils

payload = fetch_json(API_URL)

mssparkutils.fs.mkdirs(BASE_DIR)
raw_path = f"{BASE_DIR}/dstb_raw_{TIMESTAMP}.json"
mssparkutils.fs.put(raw_path, json.dumps(payload, ensure_ascii=False, indent=2), True)
print("Đã ghi RAW JSON:", raw_path)

# COMMAND ----------
# 4) Chuẩn hoá dữ liệu vào Spark DataFrame
# - Nếu API trả mảng ở root: dùng trực tiếp.
# - Nếu API trả object có key 'data' chứa mảng: ưu tiên lấy payload['data'].
if isinstance(payload, list):
    rows = payload
elif isinstance(payload, dict):
    rows = payload.get("data", [payload])
else:
    raise Exception("Định dạng JSON không hỗ trợ (root phải là list hoặc dict)")

try:
    df = spark.createDataFrame(rows)
except Exception:
    # fallback cho JSON lồng sâu/phức tạp
    rdd = spark.sparkContext.parallelize([json.dumps(x, ensure_ascii=False) for x in rows])
    df = spark.read.json(rdd)

print("Schema:")
df.printSchema()
print("Sample:")
df.show(10, truncate=False)

# COMMAND ----------
# 5) Ghi JSON Lines & Parquet vào Lakehouse (để phân tích tiếp)
normalized_dir = f"{BASE_DIR}/normalized/dt={TIMESTAMP}"
parquet_dir    = f"{BASE_DIR}/parquet/dt={TIMESTAMP}"

(df.coalesce(1)
   .write
   .mode("overwrite")
   .json(normalized_dir))

(df.write
   .mode("overwrite")
   .parquet(parquet_dir))

print("Đã ghi JSON Lines:", normalized_dir)
print("Đã ghi Parquet:", parquet_dir)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
