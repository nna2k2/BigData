# Hướng dẫn chạy Spark Job Local

## Cài đặt môi trường

### 1. Cài đặt PySpark

```bash
# Cài đặt PySpark
pip install pyspark

# Hoặc với các dependencies khác
pip install pyspark pandas numpy scikit-learn fuzzywuzzy python-Levenshtein
```

### 2. Tải Oracle JDBC Driver

Tải file `ojdbc8.jar` từ:
- Oracle: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html
- Maven: https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8

Đặt file `ojdbc8.jar` vào cùng thư mục với script hoặc chỉ định đường dẫn.

## Cách chạy

### Cách 1: Sử dụng script helper (Khuyến nghị)

```bash
# Chạy job thông thường
python run_spark_job_local.py

# Chạy với option merge-types
python run_spark_job_local.py --merge-types
```

### Cách 2: Chạy trực tiếp với Python

```bash
# Chạy job thông thường
python daily_gold_job_normalization_spark.py

# Chạy với option merge-types
python daily_gold_job_normalization_spark.py --merge-types
```

### Cách 3: Sử dụng spark-submit (nếu đã cài Spark)

```bash
# Nếu Spark đã được cài đặt và có trong PATH
spark-submit --jars ojdbc8.jar daily_gold_job_normalization_spark.py

# Với option merge-types
spark-submit --jars ojdbc8.jar daily_gold_job_normalization_spark.py --merge-types
```

## Cấu hình

Các thông số có thể chỉnh sửa trong file `daily_gold_job_normalization_spark.py`:

```python
# Database connection
DB_USER = "ADMIN"
DB_PASS = "Abcd12345678!"
DB_DSN = "34.126.123.190:1521/MYATP_low.adb.oraclecloud.com"

# Spark config
SPARK_MASTER = "local[*]"  # local[*] cho local, "yarn" cho cluster
```

## Kết quả

Sau khi chạy, các bảng mới sẽ được tạo trong Oracle DB:
- `LOCATION_DIMENSION_CLEAN`
- `GOLD_TYPE_DIMENSION_CLEAN`
- `GOLD_PRICE_FACT_CLEAN`

Snapshots sẽ được lưu trong thư mục `./snapshots/`

## Troubleshooting

### Lỗi: "Cannot find JDBC driver"
- Đảm bảo file `ojdbc8.jar` có trong thư mục hoặc chỉ định đường dẫn đúng

### Lỗi: "PySpark not found"
- Cài đặt PySpark: `pip install pyspark`

### Lỗi: "Connection refused" hoặc "ORA-XXXXX"
- Kiểm tra kết nối database
- Kiểm tra firewall/network
- Kiểm tra credentials trong config

### Lỗi: "Out of memory"
- Tăng memory trong `create_spark_session()`:
  ```python
  .config("spark.driver.memory", "4g")
  .config("spark.executor.memory", "4g")
  ```

## Lưu ý

- Script này tạo bảng mới với suffix `_CLEAN`, không cập nhật bảng gốc
- Dữ liệu gốc vẫn được giữ nguyên
- Có thể so sánh trước/sau bằng snapshots

