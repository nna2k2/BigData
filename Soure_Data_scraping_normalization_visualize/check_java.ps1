# Script kiểm tra Java version cho Spark
# Chạy: .\check_java.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "🔍 Kiểm tra Java version cho Spark" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Kiểm tra Java
Write-Host "`n📦 Kiểm tra Java..." -ForegroundColor Yellow
try {
    $javaVersion = java -version 2>&1 | Select-Object -First 1
    Write-Host "   Output: $javaVersion" -ForegroundColor Gray
    
    if ($javaVersion -match 'version "(\d+)\.') {
        $majorVersion = [int]$matches[1]
        Write-Host "   Version: Java $majorVersion" -ForegroundColor $(if ($majorVersion -ge 11) { "Green" } else { "Red" })
        
        if ($majorVersion -lt 11) {
            Write-Host "`n❌ Java version không đủ!" -ForegroundColor Red
            Write-Host "   Spark 3.x yêu cầu Java 11 trở lên (khuyến nghị Java 17)" -ForegroundColor Yellow
            Write-Host "   Version hiện tại: Java $majorVersion" -ForegroundColor Yellow
            Write-Host "`n💡 Giải pháp:" -ForegroundColor Yellow
            Write-Host "   1. Cài đặt Java 17 từ: https://adoptium.net/temurin/releases/?version=17" -ForegroundColor Cyan
            Write-Host "   2. Hoặc Java 11 từ: https://adoptium.net/temurin/releases/?version=11" -ForegroundColor Cyan
            Write-Host "   3. Xem hướng dẫn: HUONG_DAN_CAI_JAVA.md" -ForegroundColor Cyan
            exit 1
        } elseif ($majorVersion -lt 17) {
            Write-Host "`n⚠️ Java $majorVersion đủ để chạy Spark nhưng khuyến nghị Java 17" -ForegroundColor Yellow
        } else {
            Write-Host "`n✅ Java version đủ để chạy Spark!" -ForegroundColor Green
        }
    } else {
        Write-Host "`n⚠️ Không thể xác định Java version" -ForegroundColor Yellow
    }
} catch {
    Write-Host "`n❌ Java chưa được cài đặt hoặc không có trong PATH" -ForegroundColor Red
    Write-Host "   Lỗi: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "`n💡 Cài đặt Java 17 từ: https://adoptium.net/temurin/releases/?version=17" -ForegroundColor Cyan
    exit 1
}

# Kiểm tra JAVA_HOME
Write-Host "`n📦 Kiểm tra JAVA_HOME..." -ForegroundColor Yellow
$javaHome = $env:JAVA_HOME
if ($javaHome) {
    Write-Host "   ✅ JAVA_HOME: $javaHome" -ForegroundColor Green
    
    # Kiểm tra Java trong JAVA_HOME
    $javaExe = Join-Path $javaHome "bin\java.exe"
    if (Test-Path $javaExe) {
        $javaHomeVersion = & $javaExe -version 2>&1 | Select-Object -First 1
        Write-Host "   Version trong JAVA_HOME: $javaHomeVersion" -ForegroundColor Gray
    } else {
        Write-Host "   ⚠️ Không tìm thấy java.exe trong JAVA_HOME" -ForegroundColor Yellow
    }
} else {
    Write-Host "   ⚠️ JAVA_HOME chưa được set" -ForegroundColor Yellow
    Write-Host "   (Không bắt buộc nếu Java đã có trong PATH)" -ForegroundColor Gray
}

# Kiểm tra javac (compiler)
Write-Host "`n📦 Kiểm tra Java Compiler..." -ForegroundColor Yellow
try {
    $javacVersion = javac -version 2>&1
    Write-Host "   ✅ $javacVersion" -ForegroundColor Green
} catch {
    Write-Host "   ⚠️ javac không tìm thấy (không bắt buộc cho Spark)" -ForegroundColor Yellow
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "✅ Kiểm tra hoàn tất!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan

