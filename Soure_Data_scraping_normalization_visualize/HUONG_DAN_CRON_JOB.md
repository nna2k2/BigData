# 📅 HƯỚNG DẪN CẤU HÌNH CRON JOB CHO SPARK

## 🎯 Mục tiêu

Cấu hình Spark job chạy tự động hàng ngày và có thể kiểm tra lịch sử các jobs đã chạy.

## 📋 Các bước thực hiện

### Bước 1: Cấu hình Cron Job

Chạy script tự động:

```bash
chmod +x setup_cron_job.sh
./setup_cron_job.sh
```

Script sẽ hỏi bạn chọn lịch chạy:
- **1**: Hàng ngày lúc 7:00 sáng (khuyến nghị)
- **2**: Hàng ngày lúc 8:00 sáng
- **3**: Hàng ngày lúc 9:00 sáng
- **4**: Mỗi 6 giờ một lần
- **5**: Mỗi 12 giờ một lần
- **6**: Tùy chỉnh (bạn nhập cron expression)

### Bước 2: Kiểm tra trạng thái

```bash
chmod +x check_job_status.sh
./check_job_status.sh
```

Script này sẽ hiển thị:
- ✅ Cron job đã được cấu hình chưa
- 🔄 Process đang chạy
- 📝 Logs gần đây
- ⏰ Lần chạy cuối cùng
- 🔧 Trạng thái cron service

### Bước 3: Xem lịch sử jobs

```bash
chmod +x view_job_history.sh
./view_job_history.sh
```

Script này sẽ hiển thị:
- 📊 Tổng quan (tổng số, thành công, thất bại)
- 📋 20 jobs gần nhất với kết quả

## 📁 Cấu trúc thư mục

Sau khi chạy, sẽ có thư mục `logs/` chứa:
```
logs/
├── job_20260201_070000.log    # Log của job chạy lúc 7:00
├── job_20260202_070000.log    # Log của job chạy lúc 7:00 ngày hôm sau
├── job_20260203_070000.log
└── job_errors.log              # Tổng hợp các lỗi
```

## 🔍 Kiểm tra thủ công

### Xem cron jobs:

```bash
crontab -l
```

### Chỉnh sửa cron job:

```bash
crontab -e
```

### Xóa cron job:

```bash
crontab -l | grep -v "run_daily_job.sh" | crontab -
```

### Xem log real-time:

```bash
tail -f logs/job_YYYYMMDD_HHMMSS.log
```

### Xem log lỗi:

```bash
tail -f logs/job_errors.log
```

## ⚙️ Cron Expression

Format: `phút giờ ngày tháng thứ`

Ví dụ:
- `0 7 * * *` - Hàng ngày lúc 7:00 sáng
- `0 */6 * * *` - Mỗi 6 giờ một lần
- `0 9 * * 1-5` - Từ thứ 2 đến thứ 6 lúc 9:00 sáng
- `30 2 * * *` - Hàng ngày lúc 2:30 sáng

## 🐛 Troubleshooting

### Cron job không chạy

1. **Kiểm tra cron service:**
   ```bash
   sudo systemctl status cron
   # hoặc
   sudo systemctl status crond
   ```

2. **Khởi động cron service:**
   ```bash
   sudo systemctl start cron
   ```

3. **Kiểm tra log của cron:**
   ```bash
   # Ubuntu/Debian
   grep CRON /var/log/syslog
   
   # CentOS/RHEL
   grep CRON /var/log/cron
   ```

4. **Kiểm tra quyền execute:**
   ```bash
   chmod +x run_daily_job.sh
   ```

5. **Test chạy thủ công:**
   ```bash
   ./run_daily_job.sh
   ```

### Job chạy nhưng thất bại

1. **Xem log chi tiết:**
   ```bash
   tail -100 logs/job_YYYYMMDD_HHMMSS.log
   ```

2. **Xem log lỗi:**
   ```bash
   cat logs/job_errors.log
   ```

3. **Kiểm tra virtual environment:**
   - Đảm bảo `venv/bin/activate` tồn tại
   - Hoặc sửa `run_daily_job.sh` để không dùng venv

### Job chạy nhưng không có log

1. **Kiểm tra đường dẫn:**
   - Đảm bảo script chạy từ đúng thư mục
   - Kiểm tra `SCRIPT_DIR` trong `run_daily_job.sh`

2. **Kiểm tra quyền ghi:**
   ```bash
   ls -ld logs/
   chmod 755 logs/
   ```

## 📊 Monitoring

### Tự động gửi email khi lỗi (tùy chọn)

Sửa `run_daily_job.sh` để thêm:

```bash
if [ $EXIT_CODE -ne 0 ]; then
    echo "Job failed" | mail -s "Spark Job Failed" your-email@example.com
fi
```

### Tích hợp với monitoring tools

Có thể tích hợp với:
- Prometheus
- Grafana
- Nagios
- Zabbix

## ✅ Checklist

- [ ] Đã chạy `setup_cron_job.sh`
- [ ] Đã kiểm tra `check_job_status.sh`
- [ ] Đã test chạy thủ công `run_daily_job.sh`
- [ ] Đã kiểm tra cron service đang chạy
- [ ] Đã xem log đầu tiên để đảm bảo job chạy đúng
- [ ] Đã cấu hình email alert (nếu cần)

## 🔄 Cập nhật

Nếu muốn thay đổi lịch chạy:

```bash
./setup_cron_job.sh
```

Chọn option 1 để thay thế cron job cũ.

---

**Lưu ý**: Cron job sẽ chạy mãi mãi cho đến khi bạn xóa nó. Để dừng, chạy:

```bash
crontab -l | grep -v "run_daily_job.sh" | crontab -
```
