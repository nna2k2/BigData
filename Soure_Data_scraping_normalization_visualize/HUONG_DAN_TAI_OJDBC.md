# Hướng dẫn tải ojdbc8.jar thủ công

## Cách 1: Tải từ Oracle (Khuyến nghị)

1. **Truy cập trang download của Oracle:**
   - Link: https://www.oracle.com/database/technologies/jdbc-ucp-downloads.html
   - Hoặc: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html

2. **Đăng nhập/Đăng ký tài khoản Oracle** (miễn phí)

3. **Tải file:**
   - Tìm "Oracle JDBC Driver" version 23.x hoặc 21.x
   - Tải file `ojdbc8.jar` (khoảng 2-3 MB)

4. **Đặt file vào thư mục dự án:**
   ```
   E:\THAC SI\BIGDATA\Soure_Data_scraping_normalization_visualize\ojdbc8.jar
   ```

## Cách 2: Tải từ Maven Repository (Không cần đăng nhập)

### Option A: Dùng trình duyệt
1. Truy cập: https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8
2. Chọn version 23.2.0.0 hoặc 23.26.0.0.0
3. Click vào "Files" hoặc "Download"
4. Tải file `.jar`

### Option B: Dùng PowerShell (Windows)
```powershell
# Tải về thư mục hiện tại
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/23.2.0.0/ojdbc8-23.2.0.0.jar" -OutFile "ojdbc8.jar"
```

### Option C: Dùng curl (nếu có)
```bash
curl -L -o ojdbc8.jar "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/23.2.0.0/ojdbc8-23.2.0.0.jar"
```

## Cách 3: Lấy từ dự án khác (nếu có)

Nếu bạn đã có dự án khác dùng Oracle JDBC:
1. Tìm file `ojdbc8.jar` trong thư mục `lib/` hoặc `jars/`
2. Copy sang thư mục dự án hiện tại

## Kiểm tra file đã tải

Sau khi tải, kiểm tra:
```powershell
# Kiểm tra file có tồn tại không
Test-Path "ojdbc8.jar"

# Kiểm tra kích thước (nên khoảng 2-3 MB)
Get-Item "ojdbc8.jar" | Select-Object Name, Length
```

## Cấu hình đường dẫn trong code

Code đã được cấu hình để tự động tìm `ojdbc8.jar` trong:
1. Thư mục hiện tại: `ojdbc8.jar`
2. Thư mục script: `./ojdbc8.jar`
3. Thư mục parent: `../ojdbc8.jar`

Nếu đặt file ở chỗ khác, có thể chỉ định đường dẫn trong code hoặc qua biến môi trường.

## Link tải trực tiếp (Maven - không cần đăng nhập)

**Version 23.2.0.0:**
- https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/23.2.0.0/ojdbc8-23.2.0.0.jar

**Version 21.11.0.0:**
- https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.11.0.0/ojdbc8-21.11.0.0.jar

**Version 19.23.0.0:**
- https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/19.23.0.0/ojdbc8-19.23.0.0.jar

## Lưu ý

- File `.jar` là file Java Archive, không phải file Python
- Không cần giải nén, chỉ cần đặt file vào đúng vị trí
- Spark sẽ tự động load file khi khởi động
- Nếu gặp lỗi "ClassNotFoundException", kiểm tra lại đường dẫn file

