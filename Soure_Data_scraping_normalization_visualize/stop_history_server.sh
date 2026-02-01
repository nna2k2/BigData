#!/bin/bash
# Script dừng Spark History Server

echo "========================================"
echo "🛑 DỪNG SPARK HISTORY SERVER"
echo "========================================"
echo ""

# Tìm process History Server
HISTORY_PID=$(pgrep -f "org.apache.spark.deploy.history.HistoryServer")

if [ -z "$HISTORY_PID" ]; then
    echo "ℹ️  Spark History Server không đang chạy"
    exit 0
fi

echo "📋 Tìm thấy History Server:"
ps aux | grep "org.apache.spark.deploy.history.HistoryServer" | grep -v grep
echo ""

# Dừng History Server
echo "🛑 Đang dừng History Server (PID: $HISTORY_PID)..."
kill $HISTORY_PID

# Đợi một chút
sleep 2

# Kiểm tra lại
if pgrep -f "org.apache.spark.deploy.history.HistoryServer" > /dev/null; then
    echo "⚠️  Process vẫn còn, force kill..."
    kill -9 $HISTORY_PID
    sleep 1
fi

# Kiểm tra lần cuối
if pgrep -f "org.apache.spark.deploy.history.HistoryServer" > /dev/null; then
    echo "❌ Không thể dừng History Server"
    exit 1
else
    echo "✅ Đã dừng Spark History Server thành công"
fi

echo "========================================"

