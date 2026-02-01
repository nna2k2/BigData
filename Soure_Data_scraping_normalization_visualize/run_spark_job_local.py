#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script helper để chạy Spark job local
Sử dụng PySpark thay vì spark-submit
"""

import os
import sys
import subprocess

def check_pyspark():
    """Kiểm tra PySpark đã được cài đặt chưa."""
    try:
        import pyspark
        print(f"✅ PySpark đã được cài đặt: {pyspark.__version__}")
        return True
    except ImportError:
        print("❌ PySpark chưa được cài đặt!")
        print("\n📦 Cài đặt PySpark bằng lệnh:")
        print("   pip install pyspark")
        print("\n   Hoặc với Oracle JDBC driver:")
        print("   pip install pyspark findspark")
        return False

def check_ojdbc():
    """Kiểm tra ojdbc8.jar có tồn tại không."""
    possible_paths = [
        "ojdbc8.jar",
        "./ojdbc8.jar",
        "../ojdbc8.jar",
        os.path.join(os.path.dirname(__file__), "ojdbc8.jar")
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            print(f"✅ Tìm thấy JDBC driver: {os.path.abspath(path)}")
            return os.path.abspath(path)
    
    print("⚠️ Không tìm thấy ojdbc8.jar")
    print("   Tải về từ: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html")
    print("   Hoặc từ Maven: https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8")
    return None

def run_job(merge_types=False):
    """Chạy Spark job."""
    script_path = os.path.join(os.path.dirname(__file__), "daily_gold_job_normalization_spark.py")
    
    if not os.path.exists(script_path):
        print(f"❌ Không tìm thấy file: {script_path}")
        return False
    
    # Chạy bằng subprocess với Python
    print("\n🚀 Bắt đầu chạy Spark job...")
    print("=" * 60)
    
    try:
        cmd = [sys.executable, script_path]
        if merge_types:
            cmd.append("--merge-types")
        
        result = subprocess.run(cmd, cwd=os.path.dirname(__file__))
        
        print("\n" + "=" * 60)
        if result.returncode == 0:
            print("✅ Job hoàn tất!")
            return True
        else:
            print(f"❌ Job thất bại với mã lỗi: {result.returncode}")
            return False
        
    except Exception as e:
        print(f"\n❌ Lỗi khi chạy job: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function."""
    print("=" * 60)
    print("🔍 Kiểm tra môi trường Spark...")
    print("=" * 60)
    
    # Kiểm tra PySpark
    if not check_pyspark():
        return 1
    
    # Kiểm tra JDBC driver
    ojdbc_path = check_ojdbc()
    
    print("\n" + "=" * 60)
    
    # Parse arguments
    merge_types = "--merge-types" in sys.argv
    
    # Chạy job
    success = run_job(merge_types=merge_types)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())

