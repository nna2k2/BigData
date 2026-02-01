# 🚀 QUICK START: Spark History Server

## Vấn đề

Khi chạy `./start_history_server.sh`, có thể gặp lỗi vì PySpark không có sẵn script `start-history-server.sh`.

## Giải pháp

Đã tạo script Python riêng (`start_history_server.py`) để khởi động History Server một cách đáng tin cậy hơn.

## Cách chạy

### Cách 1: Dùng script bash (khuyến nghị)

```bash
chmod +x start_history_server.sh
./start_history_server.sh
```

Script bash sẽ tự động gọi script Python.

### Cách 2: Chạy trực tiếp Python script

```bash
python3 start_history_server.py
```

## Kiểm tra

Sau khi chạy, kiểm tra:

```bash
# Kiểm tra process
pgrep -f "org.apache.spark.deploy.history.HistoryServer"

# Xem log
tail -f logs/history_server.log
```

## Truy cập

Sau khi khởi động thành công, truy cập:

```
http://<SERVER_IP>:18080
```

## Nếu vẫn lỗi

1. **Kiểm tra Java:**
   ```bash
   java -version
   echo $JAVA_HOME
   ```

2. **Kiểm tra PySpark:**
   ```bash
   python3 -c "import pyspark; print(pyspark.__version__)"
   ```

3. **Kiểm tra thư mục jars:**
   ```bash
   python3 -c "import pyspark; import os; print(os.path.join(os.path.dirname(os.path.dirname(pyspark.__file__)), 'jars'))"
   ls -la <thư_mục_jars>
   ```

4. **Xem log chi tiết:**
   ```bash
   tail -50 logs/history_server.log
   ```

## Lưu ý

- History Server cần Java để chạy
- PySpark phải có đầy đủ JAR files trong thư mục `jars/`
- Port 18080 cần được mở trên firewall

