#!/bin/bash
# Script tự động mở port 4040 cho Spark UI
# Chạy: sudo ./open_spark_ui_port.sh

echo "🔓 Đang mở port 4040 cho Spark UI..."
echo ""

# Kiểm tra và mở port bằng ufw
if command -v ufw &> /dev/null; then
    echo "📦 Phát hiện ufw..."
    sudo ufw allow 4040/tcp
    echo "✅ Đã mở port 4040 bằng ufw"
    echo ""
fi

# Kiểm tra và mở port bằng iptables
if command -v iptables &> /dev/null; then
    echo "📦 Phát hiện iptables..."
    # Kiểm tra xem rule đã có chưa
    if ! sudo iptables -C INPUT -p tcp --dport 4040 -j ACCEPT 2>/dev/null; then
        sudo iptables -A INPUT -p tcp --dport 4040 -j ACCEPT
        echo "✅ Đã mở port 4040 bằng iptables"
    else
        echo "ℹ️  Port 4040 đã được mở trong iptables"
    fi
    echo ""
fi

# Kiểm tra và mở port bằng firewall-cmd (CentOS/RHEL)
if command -v firewall-cmd &> /dev/null; then
    echo "📦 Phát hiện firewall-cmd..."
    sudo firewall-cmd --permanent --add-port=4040/tcp
    sudo firewall-cmd --reload
    echo "✅ Đã mở port 4040 bằng firewall-cmd"
    echo ""
fi

# Hiển thị IP của server
echo "📡 IP của server:"
SERVER_IP=$(hostname -I | awk '{print $1}')
if [ -z "$SERVER_IP" ]; then
    # Fallback: dùng hostname
    SERVER_IP=$(hostname)
fi
echo "   $SERVER_IP"
echo ""

# Kiểm tra port có đang listen không
echo "🔍 Kiểm tra port 4040:"
if command -v netstat &> /dev/null; then
    if sudo netstat -tuln | grep -q ":4040 "; then
        echo "   ✅ Port 4040 đang được sử dụng"
        sudo netstat -tuln | grep ":4040 "
    else
        echo "   ⚠️  Port 4040 chưa được sử dụng (Spark chưa chạy)"
    fi
elif command -v ss &> /dev/null; then
    if sudo ss -tuln | grep -q ":4040 "; then
        echo "   ✅ Port 4040 đang được sử dụng"
        sudo ss -tuln | grep ":4040 "
    else
        echo "   ⚠️  Port 4040 chưa được sử dụng (Spark chưa chạy)"
    fi
fi
echo ""

# Hiển thị hướng dẫn truy cập
echo "🌐 Truy cập Spark UI tại:"
echo "   http://$SERVER_IP:4040"
echo ""
echo "📝 Lưu ý:"
echo "   - Spark UI chỉ hoạt động khi job đang chạy"
echo "   - Nếu không truy cập được, kiểm tra firewall và network"
echo ""

