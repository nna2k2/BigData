#!/bin/bash
# Script kiểm tra trạng thái Spark job

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"

echo "========================================"
echo "📊 KIỂM TRA TRẠNG THÁI SPARK JOB"
echo "========================================"
echo ""

# Kiểm tra cron job
echo "📅 Cron jobs:"
if crontab -l 2>/dev/null | grep -q "run_daily_job.sh"; then
    echo "✅ Cron job đã được cấu hình:"
    crontab -l | grep "run_daily_job.sh"
else
    echo "❌ Chưa có cron job được cấu hình"
fi
echo ""

# Kiểm tra process đang chạy
echo "🔄 Process đang chạy:"
if pgrep -f "daily_gold_job_normalization_spark.py" > /dev/null; then
    echo "✅ Spark job đang chạy:"
    ps aux | grep "daily_gold_job_normalization_spark.py" | grep -v grep
else
    echo "ℹ️  Không có Spark job đang chạy"
fi
echo ""

# Kiểm tra logs
echo "📝 Logs gần đây:"
if [ -d "$LOG_DIR" ]; then
    echo "   Log files (5 files gần nhất):"
    ls -lt "$LOG_DIR"/job_*.log 2>/dev/null | head -5 | awk '{print "   - " $9 " (" $6 " " $7 " " $8 ")"}'
    
    echo ""
    echo "   Log lỗi:"
    if [ -f "$LOG_DIR/job_errors.log" ]; then
        tail -5 "$LOG_DIR/job_errors.log"
    else
        echo "   ℹ️  Chưa có log lỗi"
    fi
else
    echo "   ❌ Thư mục logs chưa tồn tại"
fi
echo ""

# Kiểm tra lần chạy cuối cùng
echo "⏰ Lần chạy cuối cùng:"
if [ -d "$LOG_DIR" ] && [ -n "$(ls -A $LOG_DIR/job_*.log 2>/dev/null)" ]; then
    LATEST_LOG=$(ls -t "$LOG_DIR"/job_*.log 2>/dev/null | head -1)
    if [ -n "$LATEST_LOG" ]; then
        echo "   File: $(basename $LATEST_LOG)"
        echo "   Thời gian: $(stat -c %y "$LATEST_LOG" 2>/dev/null | cut -d'.' -f1)"
        echo ""
        echo "   Kết quả:"
        if tail -1 "$LATEST_LOG" | grep -q "✅ Job hoàn tất thành công"; then
            echo "   ✅ Thành công"
        elif tail -1 "$LATEST_LOG" | grep -q "❌ Job thất bại"; then
            echo "   ❌ Thất bại"
        else
            echo "   ⏳ Đang chạy hoặc chưa kết thúc"
        fi
    fi
else
    echo "   ℹ️  Chưa có log nào"
fi
echo ""

# Kiểm tra cron service
echo "🔧 Cron service:"
if systemctl is-active --quiet cron 2>/dev/null || systemctl is-active --quiet crond 2>/dev/null; then
    echo "   ✅ Cron service đang chạy"
else
    echo "   ⚠️  Cron service có thể không chạy"
    echo "   Chạy: sudo systemctl status cron"
fi
echo ""

echo "========================================"

