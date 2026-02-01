#!/bin/bash
# Script tự động setup môi trường trên server
# Chạy: bash deploy_to_server.sh

set -e  # Dừng nếu có lỗi

echo "========================================"
echo "🚀 Setup Spark ETL Job trên Server"
echo "========================================"

# Bước 1: Kiểm tra Java
echo ""
echo "📦 Bước 1: Kiểm tra Java..."
if command -v java &> /dev/null; then
    JAVA_VERSION=$(java -version 2>&1 | head -n 1)
    echo "✅ $JAVA_VERSION"
    
    # Kiểm tra version
    if java -version 2>&1 | grep -q "version \"1[1-9]\|version \"[2-9]"; then
        echo "✅ Java version đủ để chạy Spark"
    else
        echo "⚠️ Java version có thể không đủ. Cần Java 11+"
        echo "   Cài đặt: sudo apt install openjdk-17-jdk"
    fi
else
    echo "❌ Java chưa được cài đặt"
    echo "   Cài đặt: sudo apt install openjdk-17-jdk"
    exit 1
fi

# Bước 2: Kiểm tra Python
echo ""
echo "📦 Bước 2: Kiểm tra Python..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo "✅ $PYTHON_VERSION"
else
    echo "❌ Python3 chưa được cài đặt"
    echo "   Cài đặt: sudo apt install python3 python3-pip"
    exit 1
fi

# Bước 3: Tạo virtual environment
echo ""
echo "📦 Bước 3: Tạo virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✅ Đã tạo virtual environment"
else
    echo "✅ Virtual environment đã tồn tại"
fi

# Bước 4: Activate và cài đặt dependencies
echo ""
echo "📦 Bước 4: Cài đặt dependencies..."
source venv/bin/activate

if [ -f "requirements_spark.txt" ]; then
    pip install --upgrade pip
    pip install -r requirements_spark.txt
    echo "✅ Đã cài đặt dependencies từ requirements_spark.txt"
else
    echo "⚠️ Không tìm thấy requirements_spark.txt"
    echo "   Cài đặt thủ công: pip install pyspark pandas numpy scikit-learn fuzzywuzzy python-Levenshtein"
    pip install pyspark pandas numpy scikit-learn fuzzywuzzy python-Levenshtein
fi

# Bước 5: Kiểm tra ojdbc8.jar
echo ""
echo "📦 Bước 5: Kiểm tra ojdbc8.jar..."
if [ -f "ojdbc8.jar" ]; then
    echo "✅ Tìm thấy ojdbc8.jar"
    ls -lh ojdbc8.jar
else
    echo "⚠️ Không tìm thấy ojdbc8.jar"
    echo "   Vui lòng tải và đặt vào thư mục hiện tại"
    echo "   Xem: HUONG_DAN_TAI_OJDBC.md"
fi

# Bước 6: Tạo thư mục snapshots
echo ""
echo "📦 Bước 6: Tạo thư mục snapshots..."
mkdir -p snapshots
chmod 755 snapshots
echo "✅ Đã tạo thư mục snapshots"

# Bước 7: Kiểm tra file Python
echo ""
echo "📦 Bước 7: Kiểm tra file Python..."
if [ -f "daily_gold_job_normalization_spark.py" ]; then
    echo "✅ Tìm thấy daily_gold_job_normalization_spark.py"
else
    echo "❌ Không tìm thấy daily_gold_job_normalization_spark.py"
    exit 1
fi

# Bước 8: Test kết nối database (optional)
echo ""
echo "📦 Bước 8: Test kết nối database (optional)..."
read -p "Bạn có muốn test kết nối database? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    python3 << EOF
try:
    import oracledb
    conn = oracledb.connect(
        user='CLOUD',
        password='cloud123',
        dsn='136.110.60.196:1521/XEPDB1'
    )
    print("✅ Kết nối database thành công!")
    conn.close()
except Exception as e:
    print(f"❌ Lỗi kết nối: {e}")
    print("   Kiểm tra lại thông tin kết nối trong file config")
EOF
fi

echo ""
echo "========================================"
echo "✅ Setup hoàn tất!"
echo "========================================"
echo ""
echo "📝 Các bước tiếp theo:"
echo "   1. Kiểm tra config trong daily_gold_job_normalization_spark.py"
echo "   2. Chạy thử: python3 daily_gold_job_normalization_spark.py"
echo "   3. Xem hướng dẫn: HUONG_DAN_CHAY_TREN_SERVER.md"
echo ""

