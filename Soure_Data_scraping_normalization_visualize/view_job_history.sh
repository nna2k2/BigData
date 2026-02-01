#!/bin/bash
# Script xem lịch sử các Spark jobs đã chạy

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"

echo "========================================"
echo "📜 LỊCH SỬ SPARK JOBS"
echo "========================================"
echo ""

if [ ! -d "$LOG_DIR" ] || [ -z "$(ls -A $LOG_DIR/job_*.log 2>/dev/null)" ]; then
    echo "ℹ️  Chưa có log nào"
    exit 0
fi

# Đếm tổng số jobs
TOTAL_JOBS=$(ls -1 "$LOG_DIR"/job_*.log 2>/dev/null | wc -l)
SUCCESS_JOBS=$(grep -l "✅ Job hoàn tất thành công" "$LOG_DIR"/job_*.log 2>/dev/null | wc -l)
FAILED_JOBS=$(grep -l "❌ Job thất bại" "$LOG_DIR"/job_*.log 2>/dev/null | wc -l)

echo "📊 Tổng quan:"
echo "   Tổng số jobs: $TOTAL_JOBS"
echo "   Thành công: $SUCCESS_JOBS"
echo "   Thất bại: $FAILED_JOBS"
echo ""

# Hiển thị 20 jobs gần nhất
echo "📋 20 jobs gần nhất:"
echo ""
printf "%-20s %-10s %-30s\n" "Thời gian" "Kết quả" "File log"
echo "----------------------------------------------------------------"

ls -t "$LOG_DIR"/job_*.log 2>/dev/null | head -20 | while read logfile; do
    filename=$(basename "$logfile")
    timestamp=$(stat -c %y "$logfile" 2>/dev/null | cut -d'.' -f1)
    
    if grep -q "✅ Job hoàn tất thành công" "$logfile" 2>/dev/null; then
        result="✅ Thành công"
    elif grep -q "❌ Job thất bại" "$logfile" 2>/dev/null; then
        result="❌ Thất bại"
    else
        result="⏳ Chưa xong"
    fi
    
    printf "%-20s %-10s %-30s\n" "$timestamp" "$result" "$filename"
done

echo ""
echo "========================================"
echo ""
echo "💡 Để xem chi tiết một job:"
echo "   tail -f $LOG_DIR/job_YYYYMMDD_HHMMSS.log"
echo ""

