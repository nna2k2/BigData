#!/bin/bash
# Script tự động mở port 4040 cho Spark UI
# Chạy: sudo ./open_spark_ui_port.sh

echo "🔓 Đang mở port 4040 cho Spark UI và 18080 cho History Server..."
echo ""

# Kiểm tra và mở port bằng ufw
if command -v ufw &> /dev/null; then
    echo "📦 Phát hiện ufw..."
    sudo ufw allow 4040/tcp
    sudo ufw allow 18080/tcp
    echo "✅ Đã mở port 4040 (Spark UI) và 18080 (History Server) bằng ufw"
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
    if ! sudo iptables -C INPUT -p tcp --dport 18080 -j ACCEPT 2>/dev/null; then
        sudo iptables -A INPUT -p tcp --dport 18080 -j ACCEPT
        echo "✅ Đã mở port 18080 bằng iptables"
    else
        echo "ℹ️  Port 18080 đã được mở trong iptables"
    fi
    echo ""
fi

# Kiểm tra và mở port bằng firewall-cmd (CentOS/RHEL)
if command -v firewall-cmd &> /dev/null; then
    echo "📦 Phát hiện firewall-cmd..."
    sudo firewall-cmd --permanent --add-port=4040/tcp
    sudo firewall-cmd --permanent --add-port=18080/tcp
    sudo firewall-cmd --reload
    echo "✅ Đã mở port 4040 (Spark UI) và 18080 (History Server) bằng firewall-cmd"
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
echo "🔍 Kiểm tra ports:"
if command -v netstat &> /dev/null; then
    if sudo netstat -tuln | grep -q ":4040 "; then
        echo "   ✅ Port 4040 (Spark UI) đang được sử dụng"
        sudo netstat -tuln | grep ":4040 "
    else
        echo "   ⚠️  Port 4040 chưa được sử dụng (Spark job chưa chạy)"
    fi
    if sudo netstat -tuln | grep -q ":18080 "; then
        echo "   ✅ Port 18080 (History Server) đang được sử dụng"
        sudo netstat -tuln | grep ":18080 "
    else
        echo "   ⚠️  Port 18080 chưa được sử dụng (History Server chưa chạy)"
    fi
elif command -v ss &> /dev/null; then
    if sudo ss -tuln | grep -q ":4040 "; then
        echo "   ✅ Port 4040 (Spark UI) đang được sử dụng"
        sudo ss -tuln | grep ":4040 "
    else
        echo "   ⚠️  Port 4040 chưa được sử dụng (Spark job chưa chạy)"
    fi
    if sudo ss -tuln | grep -q ":18080 "; then
        echo "   ✅ Port 18080 (History Server) đang được sử dụng"
        sudo ss -tuln | grep ":18080 "
    else
        echo "   ⚠️  Port 18080 chưa được sử dụng (History Server chưa chạy)"
    fi
fi
echo ""

# Hiển thị hướng dẫn truy cập
echo "🌐 Truy cập Spark UI:"
echo "   - Spark UI (job đang chạy): http://$SERVER_IP:4040"
echo "   - History Server (lịch sử): http://$SERVER_IP:18080"
echo ""
echo "📝 Lưu ý:"
echo "   - Spark UI (4040) chỉ hoạt động khi job đang chạy"
echo "   - History Server (18080) cần khởi động riêng: ./start_history_server.sh"
echo "   - Nếu không truy cập được, kiểm tra firewall và network"
echo ""

