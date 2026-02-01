# 📊 HƯỚNG DẪN XEM LỊCH SỬ JOBS TRONG SPARK UI

## 🎯 Tổng quan

Spark UI mặc định chỉ hiển thị job **đang chạy** hoặc **vừa kết thúc**. Để xem lịch sử các jobs đã chạy hàng ngày, cần sử dụng **Spark History Server**.

## 🔧 Cấu hình

### Bước 1: Code đã tự động cấu hình

Code `daily_gold_job_normalization_spark.py` đã được cấu hình để:
- ✅ Enable event logging
- ✅ Lưu event logs vào thư mục `spark-events/`
- ✅ Nén event logs để tiết kiệm dung lượng

### Bước 2: Khởi động Spark History Server

```bash
chmod +x start_history_server.sh
./start_history_server.sh
```

Script sẽ:
- Tự động tìm PySpark installation
- Tạo thư mục `spark-events/` nếu chưa có
- Khởi động History Server trên port 18080
- Hiển thị URL để truy cập

### Bước 3: Truy cập Spark History Server

Sau khi khởi động, truy cập:

```
http://<SERVER_IP>:18080
```

Ví dụ:
```
http://136.110.60.196:18080
```

## 📋 So sánh Spark UI vs History Server

| Tính năng | Spark UI (Port 4040) | History Server (Port 18080) |
|-----------|---------------------|------------------------------|
| **Job đang chạy** | ✅ Có | ❌ Không |
| **Job vừa kết thúc** | ✅ Có (tạm thời) | ✅ Có |
| **Lịch sử jobs** | ❌ Không | ✅ Có (tất cả) |
| **Jobs hàng ngày** | ❌ Không | ✅ Có |
| **Truy cập từ bên ngoài** | ✅ Có (đã cấu hình) | ✅ Có (cần mở port 18080) |

## 🌐 Truy cập từ bên ngoài

### Mở port 18080 trên firewall:

```bash
# Ubuntu/Debian
sudo ufw allow 18080/tcp

# CentOS/RHEL
sudo firewall-cmd --permanent --add-port=18080/tcp
sudo firewall-cmd --reload

# Hoặc dùng script
sudo ./open_spark_ui_port.sh  # (cần sửa để mở port 18080)
```

### Truy cập:

```
http://<SERVER_IP>:18080
```

## 📊 Các tab trong History Server

1. **Applications**: Danh sách tất cả các Spark applications đã chạy
2. **Jobs**: Chi tiết các jobs trong mỗi application
3. **Stages**: Chi tiết các stages
4. **Storage**: Thông tin về RDD caching
5. **Environment**: Cấu hình Spark
6. **Executors**: Thông tin về executors

## 🔄 Quản lý History Server

### Khởi động:

```bash
./start_history_server.sh
```

### Dừng:

```bash
./stop_history_server.sh
```

### Kiểm tra trạng thái:

```bash
# Kiểm tra process
pgrep -f "org.apache.spark.deploy.history.HistoryServer"

# Xem log
tail -f logs/history_server.log
```

### Khởi động tự động khi boot (tùy chọn):

Thêm vào `/etc/rc.local` hoặc systemd service:

```bash
# /etc/rc.local
cd /path/to/project
./start_history_server.sh
```

## 📁 Cấu trúc thư mục

```
Soure_Data_scraping_normalization_visualize/
├── spark-events/              # Event logs (tự động tạo)
│   ├── app-20260201070000-0000
│   ├── app-20260202070000-0001
│   └── ...
├── logs/
│   └── history_server.log     # Log của History Server
├── start_history_server.sh    # Khởi động History Server
└── stop_history_server.sh     # Dừng History Server
```

## 🎯 Workflow hoàn chỉnh

### 1. Chạy job hàng ngày (cron):

```bash
# Cấu hình cron (chỉ cần làm 1 lần)
./setup_cron_job.sh
```

### 2. Khởi động History Server (chỉ cần làm 1 lần):

```bash
./start_history_server.sh
```

### 3. Xem lịch sử jobs:

Truy cập: `http://<SERVER_IP>:18080`

Bạn sẽ thấy:
- ✅ Tất cả các jobs đã chạy
- ✅ Thời gian chạy
- ✅ Trạng thái (thành công/thất bại)
- ✅ Chi tiết từng job (stages, tasks, executors)
- ✅ Performance metrics

## 🔍 Troubleshooting

### History Server không khởi động

1. **Kiểm tra PySpark đã cài:**
   ```bash
   python3 -c "import pyspark; print(pyspark.__version__)"
   ```

2. **Kiểm tra thư mục spark-events:**
   ```bash
   ls -la spark-events/
   ```

3. **Xem log:**
   ```bash
   tail -f logs/history_server.log
   ```

### Không thấy jobs trong History Server

1. **Kiểm tra event logs có được tạo không:**
   ```bash
   ls -la spark-events/
   ```

2. **Kiểm tra code đã enable event logging:**
   - Trong `daily_gold_job_normalization_spark.py`:
     - `spark.eventLog.enabled = true`
     - `spark.eventLog.dir` đã được set

3. **Kiểm tra quyền ghi:**
   ```bash
   touch spark-events/test.txt
   rm spark-events/test.txt
   ```

### Không truy cập được từ bên ngoài

1. **Kiểm tra port đã mở:**
   ```bash
   sudo netstat -tuln | grep 18080
   ```

2. **Kiểm tra firewall:**
   ```bash
   sudo ufw status | grep 18080
   ```

3. **Kiểm tra History Server đang chạy:**
   ```bash
   pgrep -f "org.apache.spark.deploy.history.HistoryServer"
   ```

## 💡 Tips

1. **Giữ History Server chạy liên tục:**
   - History Server cần chạy để xem lịch sử
   - Có thể thêm vào systemd để tự động start khi boot

2. **Dọn dẹp event logs định kỳ:**
   - Event logs có thể lớn theo thời gian
   - Có thể xóa logs cũ (ví dụ: > 30 ngày)

3. **Backup event logs:**
   - Event logs chứa thông tin quan trọng
   - Nên backup định kỳ

## ✅ Checklist

- [ ] Code đã enable event logging
- [ ] Đã khởi động History Server
- [ ] Đã mở port 18080 trên firewall
- [ ] Đã truy cập được `http://<SERVER_IP>:18080`
- [ ] Đã thấy các jobs trong History Server
- [ ] Đã cấu hình cron job chạy hàng ngày

---

**Lưu ý**: 
- Spark UI (port 4040) = Job đang chạy
- History Server (port 18080) = Lịch sử tất cả jobs

Cả hai đều cần để theo dõi đầy đủ!

