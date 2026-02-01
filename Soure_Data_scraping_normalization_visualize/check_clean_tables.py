#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script kiểm tra số lượng record trong các bảng CLEAN
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Config
DB_USER = "CLOUD"
DB_PASS = "cloud123"
DB_HOST = "136.110.60.196"
DB_PORT = "1521"
DB_SERVICE = "XEPDB1"
DB_DSN = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE}"

def create_spark_session():
    spark = SparkSession.builder \
        .appName("CheckCleanTables") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    return spark

def read_table_from_oracle(spark, table_name: str, schema: str = None):
    """Đọc bảng từ Oracle DB."""
    schema_prefix = f'"{schema}"."' if schema else '"'
    full_table = f'{schema_prefix}{table_name}"'
    
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
        .option("dbtable", full_table) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()
    return df

def main():
    spark = create_spark_session()
    
    print("=" * 60)
    print("🔍 Kiểm tra số lượng record trong các bảng")
    print("=" * 60)
    
    tables = [
        ("LOCATION_DIMENSION", "Bảng gốc"),
        ("LOCATION_DIMENSION_CLEAN", "Bảng CLEAN"),
        ("GOLD_TYPE_DIMENSION", "Bảng gốc"),
        ("GOLD_TYPE_DIMENSION_CLEAN", "Bảng CLEAN"),
        ("GOLD_PRICE_FACT", "Bảng gốc"),
        ("GOLD_PRICE_FACT_CLEAN", "Bảng CLEAN"),
    ]
    
    for table_name, table_type in tables:
        try:
            df = read_table_from_oracle(spark, table_name, DB_USER)
            count = df.count()
            print(f"✅ {table_name:40s} ({table_type:15s}): {count:>10,} records")
            
            # Hiển thị vài dòng mẫu
            if count > 0:
                print(f"   Sample data:")
                df.show(3, truncate=False)
        except Exception as e:
            print(f"❌ {table_name:40s} ({table_type:15s}): ERROR - {str(e)[:100]}")
    
    print("=" * 60)
    spark.stop()

if __name__ == "__main__":
    main()

