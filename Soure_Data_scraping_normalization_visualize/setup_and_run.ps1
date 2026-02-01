# PowerShell script để setup và chạy Spark job local
# Chạy: .\setup_and_run.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "🔍 Kiểm tra môi trường Spark..." -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Kiểm tra Java trước
Write-Host "`n📦 Kiểm tra Java..." -ForegroundColor Yellow
try {
    $javaVersion = java -version 2>&1 | Select-Object -First 1
    if ($javaVersion -match 'version "(\d+)\.') {
        $majorVersion = [int]$matches[1]
        if ($majorVersion -lt 11) {
            Write-Host "❌ Java version không đủ! (Hiện tại: Java $majorVersion)" -ForegroundColor Red
            Write-Host "   Spark yêu cầu Java 11 trở lên (khuyến nghị Java 17)" -ForegroundColor Yellow
            Write-Host "   Chạy: .\check_java.ps1 để kiểm tra chi tiết" -ForegroundColor Yellow
            Write-Host "   Xem hướng dẫn: HUONG_DAN_CAI_JAVA.md" -ForegroundColor Yellow
            exit 1
        } else {
            Write-Host "✅ $javaVersion" -ForegroundColor Green
        }
    }
} catch {
    Write-Host "❌ Java chưa được cài đặt!" -ForegroundColor Red
    Write-Host "   Cài đặt Java 17 từ: https://adoptium.net/temurin/releases/?version=17" -ForegroundColor Yellow
    exit 1
}

# Kiểm tra Python
Write-Host "`n📦 Kiểm tra Python..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "✅ $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Python chưa được cài đặt!" -ForegroundColor Red
    exit 1
}

# Kiểm tra PySpark
Write-Host "`n📦 Kiểm tra PySpark..." -ForegroundColor Yellow
try {
    python -c "import pyspark; print('PySpark:', pyspark.__version__)" 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        $pysparkVersion = python -c "import pyspark; print(pyspark.__version__)"
        Write-Host "✅ PySpark đã được cài đặt: $pysparkVersion" -ForegroundColor Green
    } else {
        throw "PySpark not found"
    }
} catch {
    Write-Host "⚠️ PySpark chưa được cài đặt" -ForegroundColor Yellow
    Write-Host "📥 Đang cài đặt PySpark và dependencies..." -ForegroundColor Yellow
    
    # Cài đặt từ requirements
    if (Test-Path "requirements_spark.txt") {
        pip install -r requirements_spark.txt
    } else {
        pip install pyspark pandas numpy scikit-learn fuzzywuzzy python-Levenshtein
    }
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Đã cài đặt PySpark thành công!" -ForegroundColor Green
    } else {
        Write-Host "❌ Lỗi khi cài đặt PySpark!" -ForegroundColor Red
        exit 1
    }
}

# Kiểm tra ojdbc8.jar
Write-Host "`n📦 Kiểm tra Oracle JDBC driver..." -ForegroundColor Yellow
$ojdbcPaths = @(
    "ojdbc8.jar",
    ".\ojdbc8.jar",
    "..\ojdbc8.jar"
)

$ojdbcFound = $false
foreach ($path in $ojdbcPaths) {
    if (Test-Path $path) {
        Write-Host "✅ Tìm thấy JDBC driver: $(Resolve-Path $path)" -ForegroundColor Green
        $ojdbcFound = $true
        break
    }
}

if (-not $ojdbcFound) {
    Write-Host "⚠️ Không tìm thấy ojdbc8.jar" -ForegroundColor Yellow
    Write-Host "   Tải về từ: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html" -ForegroundColor Yellow
    Write-Host "   Hoặc từ Maven: https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8" -ForegroundColor Yellow
}

# Chạy job
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "🚀 Bắt đầu chạy Spark job..." -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$mergeTypes = $args -contains "--merge-types"

if ($mergeTypes) {
    Write-Host "`n📝 Chạy với option --merge-types" -ForegroundColor Yellow
    python daily_gold_job_normalization_spark.py --merge-types
} else {
    python daily_gold_job_normalization_spark.py
}

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "✅ Job hoàn tất!" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Cyan
} else {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "❌ Job thất bại!" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Cyan
    exit 1
}

