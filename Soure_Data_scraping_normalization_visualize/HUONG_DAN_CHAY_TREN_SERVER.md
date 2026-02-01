# Hướng dẫn chạy Spark Job trên Server Linux

## Bước 1: Chuẩn bị môi trường trên Server

### 1.1. Kiểm tra Java

```bash
# Kiểm tra Java version
java -version

# Phải là Java 11 trở lên (khuyến nghị Java 17)
# Nếu chưa có, cài đặt:
sudo apt update
sudo apt install openjdk-17-jdk -y

# Hoặc với yum (CentOS/RHEL):
sudo yum install java-17-openjdk-devel -y

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# Hoặc tìm đường dẫn:
sudo update-alternatives --config java
```

### 1.2. Cài đặt Python và pip

```bash
# Kiểm tra Python
python3 --version

# Cài đặt pip nếu chưa có
sudo apt install python3-pip -y
# Hoặc
sudo yum install python3-pip -y
```

### 1.3. Cài đặt PySpark và dependencies

```bash
# Tạo virtual environment (khuyến nghị)
python3 -m venv venv
source venv/bin/activate

# Cài đặt dependencies
pip install pyspark pandas numpy scikit-learn fuzzywuzzy python-Levenshtein

# Hoặc từ file requirements
pip install -r requirements_spark.txt
```

## Bước 2: Upload files lên Server

### 2.1. Tạo thư mục trên server

```bash
# SSH vào server
ssh user@your-server-ip

# Tạo thư mục dự án
mkdir -p ~/gold_etl_job
cd ~/gold_etl_job
```

### 2.2. Upload files từ máy local

**Cách 1: Dùng SCP (từ máy Windows)**

```powershell
# Mở PowerShell trên máy local
cd "E:\THAC SI\BIGDATA\Soure_Data_scraping_normalization_visualize"

# Upload file Python
scp daily_gold_job_normalization_spark.py user@server-ip:~/gold_etl_job/

# Upload requirements
scp requirements_spark.txt user@server-ip:~/gold_etl_job/

# Upload ojdbc8.jar
scp ojdbc8.jar user@server-ip:~/gold_etl_job/
```

**Cách 2: Dùng WinSCP hoặc FileZilla**

- Kết nối SFTP đến server
- Upload các file:
  - `daily_gold_job_normalization_spark.py`
  - `requirements_spark.txt`
  - `ojdbc8.jar`

**Cách 3: Dùng Git (nếu có repo)**

```bash
# Trên server
git clone your-repo-url
cd your-repo/Soure_Data_scraping_normalization_visualize
```

## Bước 3: Cấu hình trên Server

### 3.1. Kiểm tra và chỉnh sửa config

```bash
# Mở file config
nano daily_gold_job_normalization_spark.py

# Kiểm tra các thông số:
# - DB_USER = "CLOUD"
# - DB_PASS = "cloud123"
# - DB_HOST = "136.110.60.196"
# - DB_PORT = "1521"
# - DB_SERVICE = "XEPDB1"
```

### 3.2. Tạo thư mục snapshots

```bash
mkdir -p snapshots
chmod 755 snapshots
```

### 3.3. Kiểm tra kết nối database

```bash
# Test kết nối Oracle (nếu có sqlplus)
sqlplus CLOUD/cloud123@136.110.60.196:1521/XEPDB1

# Hoặc test từ Python
python3 -c "
import oracledb
conn = oracledb.connect(user='CLOUD', password='cloud123', 
                        dsn='136.110.60.196:1521/XEPDB1')
print('✅ Kết nối thành công!')
conn.close()
"
```

## Bước 4: Chạy Job

### 4.1. Chạy thử lần đầu (test)

```bash
# Activate virtual environment (nếu dùng)
source venv/bin/activate

# Chạy job
python3 daily_gold_job_normalization_spark.py

# Hoặc với spark-submit (nếu có Spark standalone)
spark-submit --jars ojdbc8.jar daily_gold_job_normalization_spark.py
```

### 4.2. Chạy với option merge-types

```bash
python3 daily_gold_job_normalization_spark.py --merge-types
```

### 4.3. Chạy trong background (nếu job lâu)

```bash
# Chạy và log ra file
nohup python3 daily_gold_job_normalization_spark.py > job.log 2>&1 &

# Xem log
tail -f job.log

# Kiểm tra process
ps aux | grep python
```

## Bước 5: Tạo Cron Job (Tự động chạy hàng ngày)

### 5.1. Tạo script wrapper

```bash
# Tạo file script
nano ~/gold_etl_job/run_job.sh

# Nội dung:
#!/bin/bash
cd ~/gold_etl_job
source venv/bin/activate  # Nếu dùng venv
python3 daily_gold_job_normalization_spark.py >> job_cron.log 2>&1

# Cho phép execute
chmod +x ~/gold_etl_job/run_job.sh
```

### 5.2. Thêm vào crontab

```bash
# Mở crontab
crontab -e

# Thêm dòng để chạy mỗi ngày lúc 7:00 sáng
0 7 * * * /home/user/gold_etl_job/run_job.sh

# Hoặc chạy mỗi giờ
0 * * * * /home/user/gold_etl_job/run_job.sh

# Xem crontab hiện tại
crontab -l
```

## Bước 6: Kiểm tra kết quả

### 6.1. Kiểm tra logs

```bash
# Xem log job
tail -f job.log

# Hoặc log cron
tail -f job_cron.log
```

### 6.2. Kiểm tra snapshots

```bash
# Xem các file snapshot đã tạo
ls -lh snapshots/

# Xem snapshot mới nhất
ls -lt snapshots/ | head -5
```

### 6.3. Kiểm tra database

```bash
# Kết nối và kiểm tra bảng _CLEAN đã được tạo
sqlplus CLOUD/cloud123@136.110.60.196:1521/XEPDB1 << EOF
SELECT COUNT(*) FROM LOCATION_DIMENSION_CLEAN;
SELECT COUNT(*) FROM GOLD_TYPE_DIMENSION_CLEAN;
SELECT COUNT(*) FROM GOLD_PRICE_FACT_CLEAN;
EXIT;
EOF
```

## Bước 7: Troubleshooting

### 7.1. Lỗi kết nối database

```bash
# Kiểm tra firewall
telnet 136.110.60.196 1521

# Kiểm tra network
ping 136.110.60.196

# Test kết nối JDBC
python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.read.format('jdbc').option('url', 'jdbc:oracle:thin:CLOUD/cloud123@136.110.60.196:1521/XEPDB1').option('dbtable', 'DUAL').option('driver', 'oracle.jdbc.driver.OracleDriver').load()
df.show()
"
```

### 7.2. Lỗi thiếu memory

```bash
# Tăng memory trong code hoặc khi chạy
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g

python3 daily_gold_job_normalization_spark.py
```

### 7.3. Lỗi không tìm thấy ojdbc8.jar

```bash
# Kiểm tra file có tồn tại
ls -lh ojdbc8.jar

# Chỉ định đường dẫn đầy đủ trong code
# OJDBC_JAR_PATH = "/home/user/gold_etl_job/ojdbc8.jar"
```

## Checklist trước khi chạy

- [ ] Java 11+ đã được cài đặt
- [ ] Python 3 và pip đã được cài đặt
- [ ] PySpark và dependencies đã được cài đặt
- [ ] File `daily_gold_job_normalization_spark.py` đã upload
- [ ] File `ojdbc8.jar` đã upload
- [ ] Config database đã đúng
- [ ] Kết nối database đã test thành công
- [ ] Thư mục `snapshots/` đã được tạo
- [ ] Có quyền ghi vào thư mục dự án

## Lưu ý

1. **Firewall**: Đảm bảo port 1521 (Oracle) không bị chặn
2. **Permissions**: Đảm bảo user có quyền đọc/ghi files
3. **Disk space**: Kiểm tra dung lượng đĩa trước khi chạy
4. **Timezone**: Đảm bảo timezone server đúng (cho checkpoint)
5. **Logs**: Giữ logs để debug khi có lỗi

## Liên hệ

Nếu gặp lỗi, kiểm tra:
1. Logs trong `job.log` hoặc `job_cron.log`
2. Logs của Spark (nếu có)
3. Logs database (nếu có quyền truy cập)

