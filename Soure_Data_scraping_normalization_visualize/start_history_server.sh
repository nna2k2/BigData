#!/bin/bash
# Script khởi động Spark History Server để xem lịch sử jobs

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EVENT_LOG_DIR="$SCRIPT_DIR/spark-events"
HISTORY_PORT="${SPARK_HISTORY_PORT:-18080}"

echo "========================================"
echo "🚀 KHỞI ĐỘNG SPARK HISTORY SERVER"
echo "========================================"
echo ""

# Kiểm tra thư mục event logs
if [ ! -d "$EVENT_LOG_DIR" ]; then
    echo "⚠️  Thư mục event logs chưa tồn tại: $EVENT_LOG_DIR"
    echo "   Tạo thư mục..."
    mkdir -p "$EVENT_LOG_DIR"
    echo "✅ Đã tạo thư mục"
fi

# Kiểm tra xem History Server đã chạy chưa
if pgrep -f "org.apache.spark.deploy.history.HistoryServer" > /dev/null; then
    echo "⚠️  Spark History Server đã đang chạy"
    echo "   PID: $(pgrep -f 'org.apache.spark.deploy.history.HistoryServer')"
    echo ""
    echo "   Để dừng, chạy: ./stop_history_server.sh"
    exit 1
fi

# Tìm PySpark installation
PYSPARK_PATH=$(python3 -c "import pyspark; import os; print(os.path.dirname(pyspark.__file__))" 2>/dev/null)

if [ -z "$PYSPARK_PATH" ]; then
    echo "❌ Không tìm thấy PySpark"
    echo "   Vui lòng cài đặt: pip install pyspark"
    exit 1
fi

# Tìm spark-class hoặc spark-submit
SPARK_HOME=$(python3 -c "import pyspark; import os; print(os.path.dirname(os.path.dirname(pyspark.__file__)))" 2>/dev/null)

if [ -z "$SPARK_HOME" ] || [ ! -d "$SPARK_HOME" ]; then
    echo "❌ Không tìm thấy SPARK_HOME"
    echo "   PySpark path: $PYSPARK_PATH"
    exit 1
fi

echo "✅ Tìm thấy Spark tại: $SPARK_HOME"
echo "✅ Event log directory: $EVENT_LOG_DIR"
echo "✅ History Server port: $HISTORY_PORT"
echo ""

# Tạo log file
LOG_FILE="$SCRIPT_DIR/logs/history_server.log"
mkdir -p "$SCRIPT_DIR/logs"

# Khởi động History Server
echo "🚀 Đang khởi động History Server..."
echo "   Log file: $LOG_FILE"
echo ""

cd "$SPARK_HOME"

# Sử dụng sbin/start-history-server.sh nếu có
if [ -f "sbin/start-history-server.sh" ]; then
    echo "   Sử dụng: sbin/start-history-server.sh"
    SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://$EVENT_LOG_DIR" \
    ./sbin/start-history-server.sh >> "$LOG_FILE" 2>&1 &
elif [ -f "bin/spark-class" ]; then
    echo "   Sử dụng: bin/spark-class"
    SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://$EVENT_LOG_DIR" \
    ./bin/spark-class org.apache.spark.deploy.history.HistoryServer >> "$LOG_FILE" 2>&1 &
else
    # Fallback: dùng Python để start
    echo "   Sử dụng: Python pyspark"
    python3 -c "
from pyspark import find_spark_home
import subprocess
import os
import sys

spark_home = find_spark_home()
sbin_dir = os.path.join(spark_home, 'sbin')
if os.path.exists(os.path.join(sbin_dir, 'start-history-server.sh')):
    os.chdir(spark_home)
    env = os.environ.copy()
    env['SPARK_HISTORY_OPTS'] = f'-Dspark.history.fs.logDirectory=file://$EVENT_LOG_DIR'
    subprocess.Popen(['bash', os.path.join(sbin_dir, 'start-history-server.sh')], 
                    stdout=open('$LOG_FILE', 'a'), 
                    stderr=subprocess.STDOUT,
                    env=env)
    print('History Server started')
else:
    print('Cannot find start-history-server.sh')
    sys.exit(1)
" >> "$LOG_FILE" 2>&1 &
fi

HISTORY_PID=$!
sleep 3

# Kiểm tra xem đã start thành công chưa
if pgrep -f "org.apache.spark.deploy.history.HistoryServer" > /dev/null; then
    echo "✅ Spark History Server đã khởi động thành công!"
    echo ""
    echo "📊 Thông tin:"
    echo "   PID: $(pgrep -f 'org.apache.spark.deploy.history.HistoryServer')"
    echo "   Port: $HISTORY_PORT"
    echo "   Event Log Dir: $EVENT_LOG_DIR"
    echo ""
    
    # Lấy IP server
    SERVER_IP=$(hostname -I | awk '{print $1}')
    if [ -z "$SERVER_IP" ]; then
        SERVER_IP=$(hostname)
    fi
    
    echo "🌐 Truy cập Spark History Server:"
    echo "   http://$SERVER_IP:$HISTORY_PORT"
    echo "   hoặc"
    echo "   http://localhost:$HISTORY_PORT"
    echo ""
    echo "📝 Log file: $LOG_FILE"
    echo ""
    echo "💡 Để dừng History Server:"
    echo "   ./stop_history_server.sh"
else
    echo "❌ Không thể khởi động History Server"
    echo "   Xem log: tail -f $LOG_FILE"
    exit 1
fi

echo "========================================"

