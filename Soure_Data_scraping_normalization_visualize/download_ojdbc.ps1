# PowerShell script để tải ojdbc8.jar tự động
# Chạy: .\download_ojdbc.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "📥 Tải Oracle JDBC Driver (ojdbc8.jar)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$downloadUrl = "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/23.2.0.0/ojdbc8-23.2.0.0.jar"
$outputFile = "ojdbc8.jar"
$currentDir = Get-Location

Write-Host "`n📦 Đang tải ojdbc8.jar từ Maven Repository..." -ForegroundColor Yellow
Write-Host "   URL: $downloadUrl" -ForegroundColor Gray
Write-Host "   Lưu vào: $(Join-Path $currentDir $outputFile)" -ForegroundColor Gray

try {
    # Tải file
    Invoke-WebRequest -Uri $downloadUrl -OutFile $outputFile -UseBasicParsing
    
    if (Test-Path $outputFile) {
        $fileInfo = Get-Item $outputFile
        $fileSizeMB = [math]::Round($fileInfo.Length / 1MB, 2)
        
        Write-Host "`n✅ Tải thành công!" -ForegroundColor Green
        Write-Host "   File: $outputFile" -ForegroundColor Gray
        Write-Host "   Kích thước: $fileSizeMB MB" -ForegroundColor Gray
        Write-Host "   Vị trí: $(Resolve-Path $outputFile)" -ForegroundColor Gray
        
        Write-Host "`n✅ Bạn có thể chạy Spark job ngay bây giờ!" -ForegroundColor Green
    } else {
        Write-Host "`n❌ Lỗi: File không được tải về" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "`n❌ Lỗi khi tải file:" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Write-Host "`n💡 Thử cách khác:" -ForegroundColor Yellow
    Write-Host "   1. Tải thủ công từ: https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8" -ForegroundColor Yellow
    Write-Host "   2. Hoặc từ Oracle: https://www.oracle.com/database/technologies/jdbc-ucp-downloads.html" -ForegroundColor Yellow
    Write-Host "   3. Đặt file vào thư mục: $currentDir" -ForegroundColor Yellow
    exit 1
}

Write-Host "`n========================================" -ForegroundColor Cyan

