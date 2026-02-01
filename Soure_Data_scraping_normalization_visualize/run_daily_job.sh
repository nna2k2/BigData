#!/bin/bash
# Script wrapper để chạy Spark job hàng ngày với logging

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Tạo thư mục logs nếu chưa có
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

# Tên file log với timestamp
LOG_FILE="$LOG_DIR/job_$(date +%Y%m%d_%H%M%S).log"
ERROR_LOG="$LOG_DIR/job_errors.log"

# Activate virtual environment nếu có
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

# Ghi log bắt đầu
echo "========================================" >> "$LOG_FILE"
echo "🚀 Bắt đầu Spark job: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# Chạy job và ghi log
python3 daily_gold_job_normalization_spark.py >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

# Ghi log kết thúc
echo "" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Job hoàn tất thành công: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
else
    echo "❌ Job thất bại (exit code: $EXIT_CODE): $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
    # Ghi vào error log
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Job failed with exit code $EXIT_CODE" >> "$ERROR_LOG"
    echo "Log file: $LOG_FILE" >> "$ERROR_LOG"
    tail -20 "$LOG_FILE" >> "$ERROR_LOG"
    echo "---" >> "$ERROR_LOG"
fi
echo "========================================" >> "$LOG_FILE"

# Giữ lại chỉ 30 file log gần nhất
cd "$LOG_DIR"
ls -t job_*.log 2>/dev/null | tail -n +31 | xargs -r rm

exit $EXIT_CODE
