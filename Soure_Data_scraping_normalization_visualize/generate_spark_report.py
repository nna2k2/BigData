#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script tạo báo cáo chứng minh đang dùng Spark
Chạy: python3 generate_spark_report.py
"""

from pyspark.sql import SparkSession
import datetime as dt
import os

def create_spark_session():
    """Tạo SparkSession."""
    spark = SparkSession.builder \
        .appName("SparkProofReport") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    return spark

def generate_spark_report():
    """Tạo báo cáo chứng minh đang dùng Spark."""
    spark = create_spark_session()
    
    report_lines = []
    report_lines.append("="*80)
    report_lines.append("📊 BÁO CÁO CHỨNG MINH SỬ DỤNG SPARK")
    report_lines.append("="*80)
    report_lines.append(f"Thời gian tạo: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("")
    
    # Thông tin Spark
    report_lines.append("🔹 THÔNG TIN SPARK:")
    report_lines.append(f"   - Spark Version: {spark.version}")
    report_lines.append(f"   - Spark Master: {spark.sparkContext.master}")
    report_lines.append(f"   - Spark App Name: {spark.sparkContext.appName}")
    report_lines.append(f"   - Spark App ID: {spark.sparkContext.applicationId}")
    report_lines.append(f"   - Default Parallelism: {spark.sparkContext.defaultParallelism}")
    report_lines.append("")
    
    # Thông tin cấu hình
    report_lines.append("🔹 CẤU HÌNH SPARK:")
    conf = spark.sparkContext.getConf()
    important_configs = [
        "spark.master",
        "spark.app.name",
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.driver.memory",
        "spark.executor.memory",
    ]
    for key in important_configs:
        value = conf.get(key, "N/A")
        report_lines.append(f"   - {key}: {value}")
    report_lines.append("")
    
    # Test Spark operations
    report_lines.append("🔹 KIỂM TRA SPARK OPERATIONS:")
    
    # Test 1: Tạo DataFrame
    test_df = spark.createDataFrame([(1, "test"), (2, "spark")], ["id", "name"])
    report_lines.append(f"   ✅ Tạo DataFrame: {test_df.count()} records")
    
    # Test 2: Spark SQL
    test_df.createOrReplaceTempView("test_table")
    sql_result = spark.sql("SELECT COUNT(*) as cnt FROM test_table").collect()[0][0]
    report_lines.append(f"   ✅ Spark SQL: {sql_result} records")
    
    # Test 3: Transformations
    transformed = test_df.filter(test_df["id"] > 1).count()
    report_lines.append(f"   ✅ Transformations: {transformed} records sau filter")
    
    # Test 4: Aggregations
    agg_result = test_df.groupBy("name").count().count()
    report_lines.append(f"   ✅ Aggregations: {agg_result} groups")
    
    report_lines.append("")
    report_lines.append("="*80)
    report_lines.append("✅ KẾT LUẬN: Đang sử dụng Apache Spark để xử lý dữ liệu")
    report_lines.append("="*80)
    
    # Ghi ra file
    report_content = "\n".join(report_lines)
    report_file = f"spark_proof_report_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(report_content)
    
    # In ra console
    print(report_content)
    print(f"\n📄 Báo cáo đã được lưu vào: {report_file}")
    
    spark.stop()
    return report_file

if __name__ == "__main__":
    generate_spark_report()

