# 🌐 HƯỚNG DẪN TRUY CẬP SPARK UI TỪ BÊN NGOÀI

## 📋 Tổng quan

Spark UI mặc định chỉ bind vào `localhost`, chỉ có thể truy cập từ chính máy server. Để truy cập từ máy khác (từ máy tính của bạn), cần cấu hình Spark bind vào `0.0.0.0` và mở port trên firewall.

## 🔧 Bước 1: Cấu hình Spark (Đã tự động)

Code đã được cấu hình tự động:
```python
.config("spark.driver.bindAddress", "0.0.0.0")
.config("spark.driver.host", "0.0.0.0")
```

Khi chạy job, bạn sẽ thấy thông tin:
```
✅ Spark UI: http://<SERVER_IP>:4040 hoặc http://<HOSTNAME>:4040
```

## 🔥 Bước 2: Mở port trên firewall

### Trên Linux (Ubuntu/Debian):

#### Cách 1: Dùng ufw (nếu đã cài)
```bash
# Kiểm tra firewall status
sudo ufw status

# Mở port 4040
sudo ufw allow 4040/tcp

# Kiểm tra lại
sudo ufw status
```

#### Cách 2: Dùng iptables
```bash
# Mở port 4040
sudo iptables -A INPUT -p tcp --dport 4040 -j ACCEPT

# Lưu cấu hình (tùy hệ thống)
sudo iptables-save > /etc/iptables/rules.v4
# hoặc
sudo netfilter-persistent save
```

#### Cách 3: Dùng firewall-cmd (CentOS/RHEL)
```bash
sudo firewall-cmd --permanent --add-port=4040/tcp
sudo firewall-cmd --reload
```

### Trên Windows Server:
```powershell
# Mở port 4040
New-NetFirewallRule -DisplayName "Spark UI" -Direction Inbound -LocalPort 4040 -Protocol TCP -Action Allow
```

## 🌐 Bước 3: Lấy IP của server

### Trên Linux:
```bash
# Cách 1: Dùng hostname
hostname -I

# Cách 2: Dùng ip
ip addr show

# Cách 3: Dùng ifconfig
ifconfig | grep "inet "

# Cách 4: Từ code Python (đã tự động hiển thị)
python3 -c "import socket; s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.connect(('8.8.8.8', 80)); print(s.getsockname()[0]); s.close()"
```

### Trên Windows:
```powershell
ipconfig
# Tìm IPv4 Address
```

## 🔍 Bước 4: Kiểm tra port đã mở chưa

### Từ server:
```bash
# Kiểm tra port 4040 có đang listen không
sudo netstat -tuln | grep 4040
# hoặc
sudo ss -tuln | grep 4040

# Kiểm tra từ bên ngoài (từ máy khác)
telnet <SERVER_IP> 4040
# hoặc
nc -zv <SERVER_IP> 4040
```

## 🚀 Bước 5: Truy cập Spark UI

### Từ trình duyệt:
```
http://<SERVER_IP>:4040
```

Ví dụ:
```
http://136.110.60.196:4040
```

### Lưu ý:
- Spark UI chỉ hoạt động khi job đang chạy hoặc vừa kết thúc
- Nếu job đã kết thúc lâu, Spark UI sẽ không còn
- Để giữ Spark UI lâu hơn, có thể cấu hình `spark.eventLog.enabled=true`

## 📊 Bước 6: Cấu hình Spark History Server (Tùy chọn)

Nếu muốn xem lịch sử các jobs đã chạy, cần cấu hình Spark History Server:

### 1. Tạo thư mục event logs:
```bash
mkdir -p /opt/spark/spark-events
```

### 2. Cấu hình trong code:
```python
.config("spark.eventLog.enabled", "true")
.config("spark.eventLog.dir", "file:///opt/spark/spark-events")
```

### 3. Khởi động History Server:
```bash
/opt/spark/sbin/start-history-server.sh
```

### 4. Truy cập:
```
http://<SERVER_IP>:18080
```

## 🔒 Bước 7: Bảo mật (Khuyến nghị)

Nếu server có thể truy cập từ internet, nên bảo mật Spark UI:

### Cách 1: Dùng SSH Tunnel (An toàn nhất)
```bash
# Từ máy local của bạn
ssh -L 4040:localhost:4040 user@<SERVER_IP>

# Sau đó truy cập từ trình duyệt:
http://localhost:4040
```

### Cách 2: Dùng reverse proxy với authentication
Cấu hình Nginx/Apache với basic auth trước Spark UI.

### Cách 3: Chỉ mở port cho IP cụ thể
```bash
# Chỉ cho phép IP của bạn
sudo ufw allow from <YOUR_IP> to any port 4040
```

## 🐛 Troubleshooting

### Lỗi: Không truy cập được từ bên ngoài

1. **Kiểm tra firewall:**
   ```bash
   sudo ufw status
   sudo iptables -L -n | grep 4040
   ```

2. **Kiểm tra Spark có bind đúng không:**
   ```bash
   sudo netstat -tuln | grep 4040
   # Phải thấy: 0.0.0.0:4040 hoặc :::4040
   # KHÔNG phải: 127.0.0.1:4040
   ```

3. **Kiểm tra từ server:**
   ```bash
   curl http://localhost:4040
   # Nếu OK thì Spark UI đang chạy
   ```

4. **Kiểm tra từ máy khác:**
   ```bash
   curl http://<SERVER_IP>:4040
   # Nếu timeout thì firewall chưa mở
   ```

### Lỗi: Spark UI không hiển thị

- Spark UI chỉ hiển thị khi job đang chạy
- Nếu job đã kết thúc, cần cấu hình History Server

### Lỗi: Connection refused

- Kiểm tra Spark có đang chạy không
- Kiểm tra port 4040 có bị process khác dùng không
- Kiểm tra firewall đã mở chưa

## 📝 Script tự động mở port

Tạo file `open_spark_ui_port.sh`:

```bash
#!/bin/bash
# Mở port 4040 cho Spark UI

echo "🔓 Đang mở port 4040 cho Spark UI..."

# Kiểm tra ufw
if command -v ufw &> /dev/null; then
    sudo ufw allow 4040/tcp
    echo "✅ Đã mở port 4040 bằng ufw"
fi

# Kiểm tra iptables
if command -v iptables &> /dev/null; then
    sudo iptables -A INPUT -p tcp --dport 4040 -j ACCEPT
    echo "✅ Đã mở port 4040 bằng iptables"
fi

# Kiểm tra firewall-cmd
if command -v firewall-cmd &> /dev/null; then
    sudo firewall-cmd --permanent --add-port=4040/tcp
    sudo firewall-cmd --reload
    echo "✅ Đã mở port 4040 bằng firewall-cmd"
fi

# Hiển thị IP
echo ""
echo "📡 IP của server:"
hostname -I | awk '{print $1}'

echo ""
echo "🌐 Truy cập Spark UI tại:"
echo "   http://$(hostname -I | awk '{print $1}'):4040"
```

Chạy:
```bash
chmod +x open_spark_ui_port.sh
sudo ./open_spark_ui_port.sh
```

## ✅ Checklist

- [ ] Code đã cấu hình `spark.driver.bindAddress = 0.0.0.0`
- [ ] Đã mở port 4040 trên firewall
- [ ] Đã biết IP của server
- [ ] Đã kiểm tra port đang listen
- [ ] Đã truy cập được từ trình duyệt
- [ ] (Tùy chọn) Đã cấu hình History Server
- [ ] (Tùy chọn) Đã bảo mật Spark UI

---

**Lưu ý**: Spark UI chỉ hoạt động khi job đang chạy. Nếu muốn xem lịch sử, cần cấu hình Spark History Server.

