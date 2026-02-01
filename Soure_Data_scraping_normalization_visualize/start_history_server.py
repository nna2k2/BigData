#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script khởi động Spark History Server bằng Python
Chạy: python3 start_history_server.py
"""

import os
import sys
import subprocess
import signal
from pathlib import Path

def find_spark_home():
    """Tìm SPARK_HOME từ PySpark."""
    try:
        import pyspark
        spark_home = os.path.dirname(os.path.dirname(pyspark.__file__))
        return spark_home
    except ImportError:
        print("❌ Không tìm thấy PySpark")
        print("   Vui lòng cài đặt: pip install pyspark")
        sys.exit(1)

def start_history_server():
    """Khởi động Spark History Server."""
    script_dir = Path(__file__).parent.absolute()
    event_log_dir = script_dir / "spark-events"
    log_dir = script_dir / "logs"
    
    # Tạo thư mục nếu chưa có
    event_log_dir.mkdir(exist_ok=True)
    log_dir.mkdir(exist_ok=True)
    
    # Tìm SPARK_HOME
    spark_home = find_spark_home()
    print(f"✅ Tìm thấy Spark tại: {spark_home}")
    print(f"✅ Event log directory: {event_log_dir}")
    print(f"✅ History Server port: 18080")
    print("")
    
    # Kiểm tra xem History Server đã chạy chưa
    try:
        result = subprocess.run(
            ["pgrep", "-f", "org.apache.spark.deploy.history.HistoryServer"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            pid = result.stdout.strip()
            print(f"⚠️  Spark History Server đã đang chạy (PID: {pid})")
            print("   Để dừng, chạy: ./stop_history_server.sh")
            return
    except:
        pass
    
    # Tìm spark-class hoặc sbin script
    spark_class = os.path.join(spark_home, "bin", "spark-class")
    start_script = os.path.join(spark_home, "sbin", "start-history-server.sh")
    
    log_file = log_dir / "history_server.log"
    
    print("🚀 Đang khởi động History Server...")
    print(f"   Log file: {log_file}")
    print("")
    
    # Set environment variables
    env = os.environ.copy()
    env["SPARK_HOME"] = spark_home
    env["SPARK_HISTORY_OPTS"] = f"-Dspark.history.fs.logDirectory=file://{event_log_dir.absolute()}"
    
    # Thử dùng start-history-server.sh trước
    if os.path.exists(start_script):
        print("   Sử dụng: sbin/start-history-server.sh")
        os.chdir(spark_home)
        with open(log_file, "a") as f:
            process = subprocess.Popen(
                ["bash", start_script],
                stdout=f,
                stderr=subprocess.STDOUT,
                env=env,
                cwd=spark_home
            )
    elif os.path.exists(spark_class):
        print("   Sử dụng: bin/spark-class")
        os.chdir(spark_home)
        with open(log_file, "a") as f:
            process = subprocess.Popen(
                [spark_class, "org.apache.spark.deploy.history.HistoryServer"],
                stdout=f,
                stderr=subprocess.STDOUT,
                env=env,
                cwd=spark_home
            )
    else:
        # Fallback: dùng pyspark để tìm và chạy
        print("   Sử dụng: pyspark (fallback - tìm jars)")
        try:
            import pyspark
            # Tìm jar file trong PySpark installation
            jars_dir = os.path.join(spark_home, "jars")
            
            if not os.path.exists(jars_dir):
                print(f"❌ Không tìm thấy thư mục jars: {jars_dir}")
                print("   PySpark có thể không có đầy đủ files")
                print("   Thử cài đặt Spark standalone hoặc dùng cách khác")
                sys.exit(1)
            
            # Sử dụng java trực tiếp với spark jars
            java_home = os.environ.get("JAVA_HOME", "")
            if java_home:
                java_bin = os.path.join(java_home, "bin", "java")
            else:
                java_bin = "java"
            
            # Kiểm tra java có tồn tại không
            try:
                subprocess.run([java_bin, "-version"], capture_output=True, check=True)
            except:
                print(f"❌ Không tìm thấy Java: {java_bin}")
                print("   Vui lòng cài đặt Java và set JAVA_HOME")
                sys.exit(1)
            
            # Tìm tất cả jar files
            spark_jars = []
            if os.path.exists(jars_dir):
                for jar in os.listdir(jars_dir):
                    if jar.endswith(".jar"):
                        spark_jars.append(os.path.join(jars_dir, jar))
            
            if not spark_jars:
                print(f"❌ Không tìm thấy Spark JAR files trong: {jars_dir}")
                print("   PySpark có thể không có đầy đủ files")
                sys.exit(1)
            
            print(f"   Tìm thấy {len(spark_jars)} JAR files")
            
            # Tạo classpath
            classpath = ":".join(spark_jars)
            
            # Chạy History Server
            os.chdir(spark_home)
            cmd = [
                java_bin,
                f"-Dspark.history.fs.logDirectory=file://{event_log_dir.absolute()}",
                "-cp", classpath,
                "org.apache.spark.deploy.history.HistoryServer"
            ]
            
            print(f"   Command: {' '.join(cmd[:3])} ...")
            with open(log_file, "a") as f:
                f.write(f"\n{'='*60}\n")
                f.write(f"Starting History Server at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Command: {' '.join(cmd)}\n")
                f.write(f"{'='*60}\n")
                process = subprocess.Popen(
                    cmd,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    env=env,
                    cwd=spark_home
                )
        except Exception as e:
            print(f"❌ Lỗi khi khởi động History Server: {e}")
            print(f"   Xem log: tail -f {log_file}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    # Đợi một chút
    import time
    time.sleep(3)
    
    # Kiểm tra xem đã start thành công chưa
    try:
        result = subprocess.run(
            ["pgrep", "-f", "org.apache.spark.deploy.history.HistoryServer"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            pid = result.stdout.strip()
            print("✅ Spark History Server đã khởi động thành công!")
            print("")
            print("📊 Thông tin:")
            print(f"   PID: {pid}")
            print("   Port: 18080")
            print(f"   Event Log Dir: {event_log_dir}")
            print("")
            
            # Lấy IP server
            import socket
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                server_ip = s.getsockname()[0]
                s.close()
            except:
                server_ip = "localhost"
            
            print("🌐 Truy cập Spark History Server:")
            print(f"   http://{server_ip}:18080")
            print("   hoặc")
            print("   http://localhost:18080")
            print("")
            print(f"📝 Log file: {log_file}")
            print("")
            print("💡 Để dừng History Server:")
            print("   ./stop_history_server.sh")
        else:
            print("❌ Không thể khởi động History Server")
            print(f"   Xem log: tail -f {log_file}")
            sys.exit(1)
    except Exception as e:
        print(f"⚠️  Không thể kiểm tra trạng thái: {e}")
        print(f"   Xem log: tail -f {log_file}")

if __name__ == "__main__":
    start_history_server()

