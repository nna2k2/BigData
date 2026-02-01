# Quick Setup - Chạy nhanh trên Server

## Nếu gặp lỗi "venv/bin/activate: No such file or directory"

### Giải pháp 1: Tạo virtual environment (Sau khi cài python3-venv)

```bash
# Bước 1: Cài đặt python3-venv (nếu chưa có)
apt install python3-venv
# Hoặc với version cụ thể:
apt install python3.12-venv

# Bước 2: Vào thư mục dự án
cd ~/gold_etl_job/BigData/Soure_Data_scraping_normalization_visualize

# Bước 3: Tạo virtual environment
python3 -m venv venv

# Bước 4: Activate
source venv/bin/activate

# Bước 5: Cài đặt dependencies
pip install --upgrade pip
pip install pyspark pandas numpy scikit-learn fuzzywuzzy python-Levenshtein
```

### Giải pháp 2: Chạy không cần virtual environment (Khuyến nghị nếu gặp lỗi venv)

```bash
# Cài đặt trực tiếp vào system Python
pip3 install pyspark pandas numpy scikit-learn fuzzywuzzy python-Levenshtein

# Hoặc nếu cần quyền root:
# sudo pip3 install pyspark pandas numpy scikit-learn fuzzywuzzy python-Levenshtein

# Chạy job trực tiếp
python3 daily_gold_job_normalization_spark.py
```

### Giải pháp 3: Cài python3-venv và tạo lại venv

```bash
# Cài đặt python3-venv
apt update
apt install python3-venv -y

# Hoặc với version cụ thể (ví dụ Python 3.12):
apt install python3.12-venv -y

# Sau đó tạo lại venv
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install pyspark pandas numpy scikit-learn fuzzywuzzy python-Levenshtein
```

### Giải pháp 3: Dùng script tự động

```bash
# Chạy script setup
bash deploy_to_server.sh
```

## Kiểm tra các bước

```bash
# 1. Kiểm tra Python
python3 --version

# 2. Kiểm tra Java
java -version

# 3. Kiểm tra file có tồn tại
ls -la daily_gold_job_normalization_spark.py
ls -la ojdbc8.jar

# 4. Tạo thư mục snapshots
mkdir -p snapshots

# 5. Chạy job
python3 daily_gold_job_normalization_spark.py
```

