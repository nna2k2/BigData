# Sửa lỗi Virtual Environment trên Server

## Lỗi: "ensurepip is not available"

### Nguyên nhân
Thiếu package `python3-venv` trên hệ thống.

### Giải pháp nhanh

```bash
# 1. Cài đặt python3-venv
apt update
apt install python3-venv -y

# Hoặc với version cụ thể (ví dụ Python 3.12):
apt install python3.12-venv -y

# 2. Tạo lại virtual environment
python3 -m venv venv

# 3. Activate
source venv/bin/activate

# 4. Cài dependencies
pip install --upgrade pip
pip install pyspark pandas numpy scikit-learn fuzzywuzzy python-Levenshtein
```

## Giải pháp thay thế: Chạy không cần venv

Nếu không muốn dùng virtual environment:

```bash
# Cài đặt trực tiếp vào system Python
pip3 install pyspark pandas numpy scikit-learn fuzzywuzzy python-Levenshtein

# Chạy job
python3 daily_gold_job_normalization_spark.py
```

## Kiểm tra Python version

```bash
# Xem version Python
python3 --version

# Nếu là Python 3.12, cài:
apt install python3.12-venv -y

# Nếu là Python 3.11, cài:
apt install python3.11-venv -y

# Hoặc cài chung cho tất cả:
apt install python3-venv -y
```

## Sau khi fix

```bash
# Tạo thư mục snapshots
mkdir -p snapshots

# Chạy job
python3 daily_gold_job_normalization_spark.py
```

