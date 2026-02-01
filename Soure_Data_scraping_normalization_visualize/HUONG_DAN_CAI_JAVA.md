# Hướng dẫn cài đặt Java 17 cho Spark

## Vấn đề

Spark 3.x yêu cầu **Java 11 trở lên** (khuyến nghị Java 17).
- Java 8 (version 52.0) → ❌ Không đủ
- Java 11 (version 55.0) → ✅ Đủ
- Java 17 (version 61.0) → ✅✅ Khuyến nghị

## Cách 1: Cài đặt Java 17 (Khuyến nghị)

### Option A: Tải từ Oracle (JDK)

1. **Truy cập:** https://www.oracle.com/java/technologies/downloads/#java17
2. **Chọn:** Windows x64 Installer
3. **Tải và cài đặt** (chọn "Set JAVA_HOME variable" khi cài)

### Option B: Tải từ Adoptium (OpenJDK - Miễn phí)

1. **Truy cập:** https://adoptium.net/temurin/releases/?version=17
2. **Chọn:** Windows x64 → JDK 17 → .msi installer
3. **Tải và cài đặt**

### Option C: Dùng Chocolatey (nếu đã cài)

```powershell
# Chạy PowerShell as Administrator
choco install openjdk17
```

## Cách 2: Cài đặt Java 11 (Tối thiểu)

Nếu không thể cài Java 17, có thể dùng Java 11:

1. **Truy cập:** https://adoptium.net/temurin/releases/?version=11
2. **Tải và cài đặt** JDK 11

## Sau khi cài đặt

### 1. Kiểm tra version

```powershell
java -version
# Phải hiển thị: java version "17.x.x" hoặc "11.x.x"
```

### 2. Cấu hình JAVA_HOME

#### Nếu cài từ Oracle/Adoptium:
- Thường tự động set JAVA_HOME
- Kiểm tra: `echo $env:JAVA_HOME`

#### Nếu cần set thủ công:

```powershell
# Tìm đường dẫn Java (thường là)
# C:\Program Files\Java\jdk-17
# hoặc C:\Program Files\Eclipse Adoptium\jdk-17.x.x-hotspot

# Set JAVA_HOME (tạm thời - chỉ cho session hiện tại)
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"

# Set PATH
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

#### Set vĩnh viễn (System Environment):

1. Mở **System Properties** → **Environment Variables**
2. Thêm **JAVA_HOME** = `C:\Program Files\Java\jdk-17`
3. Thêm vào **Path**: `%JAVA_HOME%\bin`
4. **Restart** PowerShell/Terminal

### 3. Kiểm tra lại

```powershell
java -version
javac -version
echo $env:JAVA_HOME
```

## Cách 3: Dùng Java Portable (Không cần cài đặt)

Nếu không muốn cài đặt toàn hệ thống:

1. **Tải Java 17 Portable:**
   - https://adoptium.net/temurin/releases/?version=17
   - Chọn: **Windows x64** → **JDK 17** → **.zip** (không phải .msi)

2. **Giải nén** vào thư mục dự án:
   ```
   E:\THAC SI\BIGDATA\jdk-17
   ```

3. **Set JAVA_HOME trong script:**
   - Xem file `setup_and_run.ps1` (sẽ được cập nhật)

## Kiểm tra nhanh

Chạy script kiểm tra:
```powershell
.\check_java.ps1
```

## Lưu ý

- **Không xóa Java 8** nếu các ứng dụng khác cần
- Có thể cài nhiều version Java cùng lúc
- Chỉ cần set JAVA_HOME đúng version khi chạy Spark

## Troubleshooting

### Lỗi: "java is not recognized"
- Kiểm tra PATH có chứa `%JAVA_HOME%\bin`
- Restart terminal sau khi set environment variables

### Lỗi: "Wrong Java version"
- Kiểm tra: `java -version`
- Đảm bảo JAVA_HOME trỏ đúng version 17

### Nhiều version Java
- Set JAVA_HOME trước khi chạy Spark
- Hoặc dùng script tự động detect

