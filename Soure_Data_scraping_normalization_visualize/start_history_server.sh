#!/bin/bash
# Script khởi động Spark History Server để xem lịch sử jobs
# Script này sẽ gọi start_history_server.py để khởi động

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================"
echo "🚀 KHỞI ĐỘNG SPARK HISTORY SERVER"
echo "========================================"
echo ""

# Kiểm tra Python script
if [ -f "$SCRIPT_DIR/start_history_server.py" ]; then
    python3 "$SCRIPT_DIR/start_history_server.py"
    exit $?
else
    echo "❌ Không tìm thấy start_history_server.py"
    echo "   Vui lòng đảm bảo file tồn tại trong: $SCRIPT_DIR"
    exit 1
fi
