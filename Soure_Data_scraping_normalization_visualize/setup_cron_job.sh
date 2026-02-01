#!/bin/bash
# Script cấu hình cron job để chạy Spark job hàng ngày

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_SCRIPT="$SCRIPT_DIR/run_daily_job.sh"

echo "========================================"
echo "📅 CẤU HÌNH CRON JOB CHO SPARK"
echo "========================================"
echo ""

# Kiểm tra file run script
if [ ! -f "$RUN_SCRIPT" ]; then
    echo "❌ Không tìm thấy file: $RUN_SCRIPT"
    exit 1
fi

# Cho phép execute
chmod +x "$RUN_SCRIPT"
echo "✅ Đã cho phép execute: $RUN_SCRIPT"
echo ""

# Hiển thị các tùy chọn
echo "Chọn lịch chạy:"
echo "1. Hàng ngày lúc 7:00 sáng (khuyến nghị)"
echo "2. Hàng ngày lúc 8:00 sáng"
echo "3. Hàng ngày lúc 9:00 sáng"
echo "4. Mỗi 6 giờ một lần"
echo "5. Mỗi 12 giờ một lần"
echo "6. Tùy chỉnh (bạn sẽ nhập cron expression)"
echo ""
read -p "Chọn (1-6): " choice

case $choice in
    1)
        CRON_SCHEDULE="0 7 * * *"
        SCHEDULE_DESC="Hàng ngày lúc 7:00 sáng"
        ;;
    2)
        CRON_SCHEDULE="0 8 * * *"
        SCHEDULE_DESC="Hàng ngày lúc 8:00 sáng"
        ;;
    3)
        CRON_SCHEDULE="0 9 * * *"
        SCHEDULE_DESC="Hàng ngày lúc 9:00 sáng"
        ;;
    4)
        CRON_SCHEDULE="0 */6 * * *"
        SCHEDULE_DESC="Mỗi 6 giờ một lần"
        ;;
    5)
        CRON_SCHEDULE="0 */12 * * *"
        SCHEDULE_DESC="Mỗi 12 giờ một lần"
        ;;
    6)
        read -p "Nhập cron expression (ví dụ: 0 7 * * *): " CRON_SCHEDULE
        SCHEDULE_DESC="Tùy chỉnh: $CRON_SCHEDULE"
        ;;
    *)
        echo "❌ Lựa chọn không hợp lệ"
        exit 1
        ;;
esac

# Tạo cron entry
CRON_ENTRY="$CRON_SCHEDULE $RUN_SCRIPT"

# Kiểm tra xem đã có cron job chưa
if crontab -l 2>/dev/null | grep -q "$RUN_SCRIPT"; then
    echo ""
    echo "⚠️  Đã có cron job cho script này. Bạn muốn:"
    echo "1. Thay thế cron job cũ"
    echo "2. Giữ nguyên và thoát"
    read -p "Chọn (1-2): " replace_choice
    
    if [ "$replace_choice" = "1" ]; then
        # Xóa cron job cũ
        crontab -l 2>/dev/null | grep -v "$RUN_SCRIPT" | crontab -
        echo "✅ Đã xóa cron job cũ"
    else
        echo "ℹ️  Giữ nguyên cron job hiện tại"
        exit 0
    fi
fi

# Thêm cron job mới
(crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -

echo ""
echo "✅ Đã thêm cron job:"
echo "   Lịch: $SCHEDULE_DESC"
echo "   Script: $RUN_SCRIPT"
echo "   Cron: $CRON_SCHEDULE"
echo ""

# Hiển thị cron jobs hiện tại
echo "📋 Cron jobs hiện tại:"
crontab -l
echo ""

# Kiểm tra cron service
if systemctl is-active --quiet cron 2>/dev/null || systemctl is-active --quiet crond 2>/dev/null; then
    echo "✅ Cron service đang chạy"
else
    echo "⚠️  Cron service có thể không chạy. Chạy:"
    echo "   sudo systemctl start cron"
    echo "   hoặc"
    echo "   sudo systemctl start crond"
fi

echo ""
echo "========================================"
echo "✅ Hoàn tất!"
echo "========================================"
