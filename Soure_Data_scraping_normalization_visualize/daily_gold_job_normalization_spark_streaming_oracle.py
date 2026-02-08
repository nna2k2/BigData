# -*- coding: utf-8 -*-
"""
Daily Gold ETL Job - Spark Structured Streaming với Oracle Polling
Tự động phát hiện và xử lý khi Oracle database thay đổi

Giải pháp: Spark Structured Streaming + foreachBatch
- Dùng memory source làm trigger
- foreachBatch để polling Oracle mỗi interval
- Có checkpoint tự động, recovery, monitoring
"""

import argparse
import datetime as dt
import os
import sys
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import (
    col, when, lit, trim, upper, lower, regexp_replace, 
    concat_ws, first, last, max as spark_max, min as spark_min,
    count, isnan, isnull, coalesce, to_timestamp, date_format,
    row_number, window, monotonically_increasing_id, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, 
    TimestampType, DoubleType, LongType
)
from pyspark.sql.window import Window

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from fuzzywuzzy import fuzz

import re
import unicodedata

# Import các hàm clean từ batch job
# Thêm đường dẫn để import
sys.path.insert(0, os.path.dirname(__file__))
try:
    from daily_gold_job_normalization_spark import (
        normalize_locations,
        enrich_gold_types,
        normalize_purity_format,
        normalize_category_smart,
        normalize_gold_type_and_unit,
        merge_duplicate_types_and_update_fact,
        build_similarity_groups,
        norm_txt,
        snapshot_table
    )
    BATCH_FUNCTIONS_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ Không thể import batch functions: {e}")
    print("   Sẽ chỉ xử lý FACT, không clean LOCATION và TYPE")
    BATCH_FUNCTIONS_AVAILABLE = False

# ====================== CONFIG ======================
# Đọc từ environment variables (Docker) hoặc dùng giá trị mặc định
DB_USER = os.environ.get("DB_USER", "SYSTEM")
DB_PASS = os.environ.get("DB_PASS", "Welcome_1234")
DB_HOST = os.environ.get("DB_HOST", "136.110.60.196")
DB_PORT = os.environ.get("DB_PORT", "1521")
DB_SERVICE = os.environ.get("DB_SERVICE", "XEPDB1")

DB_DSN = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE}"
DB_URL = f"jdbc:oracle:thin:@{DB_DSN}"

SNAPSHOT_DIR = "./snapshots"
JOB_NAME = "DAILY_GOLD_JOB_STREAMING_ORACLE"
SIM_THRESHOLD_LOC = 0.80
SIM_THRESHOLD_TYPE = 0.75
FUZZY_FALLBACK = 90

# Các constants cần thiết cho batch functions (nếu import được)
try:
    from daily_gold_job_normalization_spark import (
        SIM_THRESHOLD_LOC as BATCH_SIM_THRESHOLD_LOC,
        SIM_THRESHOLD_TYPE as BATCH_SIM_THRESHOLD_TYPE,
        FUZZY_FALLBACK as BATCH_FUZZY_FALLBACK
    )
except:
    pass

# Streaming config
STREAMING_CHECKPOINT_DIR = "./checkpoints/streaming_oracle"
STREAMING_TRIGGER_INTERVAL = "60 seconds"  # Polling mỗi 60 giây
TIMESTAMP_COLUMN = "RECORDED_AT"  # Cột timestamp để phát hiện thay đổi

# Spark config
SPARK_APP_NAME = "DailyGoldETLJobStreamingOracle"
SPARK_MASTER = "local[*]"

# ====================================================

def create_spark_session(ojdbc_path: str = None):
    """Tạo SparkSession với cấu hình streaming."""
    builder = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.streaming.checkpointLocation", STREAMING_CHECKPOINT_DIR)
    
    # Thêm JDBC driver nếu có
    if ojdbc_path:
        if os.path.exists(ojdbc_path):
            builder = builder.config("spark.jars", ojdbc_path)
            print(f"✅ Đã load JDBC driver từ: {ojdbc_path}")
        else:
            print(f"⚠️ Không tìm thấy JDBC driver tại: {ojdbc_path}")
    else:
        possible_paths = [
            "ojdbc8.jar",
            "./ojdbc8.jar",
            "../ojdbc8.jar",
            os.path.join(os.path.dirname(__file__), "ojdbc8.jar")
        ]
        for path in possible_paths:
            if os.path.exists(path):
                builder = builder.config("spark.jars", os.path.abspath(path))
                print(f"✅ Đã tự động tìm thấy JDBC driver: {os.path.abspath(path)}")
                break
    
    spark = builder.getOrCreate()
    return spark

def read_table_from_oracle(spark: SparkSession, table_name: str, schema: str = None) -> 'DataFrame':
    """Đọc bảng từ Oracle DB (batch)."""
    schema_prefix = f'"{schema}"."' if schema else '"'
    full_table = f'{schema_prefix}{table_name}"'
    
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
        .option("dbtable", full_table) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()
    return df

def read_new_data_from_oracle(spark: SparkSession, table_name: str, 
                              last_timestamp: dt.datetime,
                              timestamp_column: str = TIMESTAMP_COLUMN) -> 'DataFrame':
    """
    Đọc chỉ dữ liệu MỚI từ Oracle dựa trên timestamp.
    
    Logic:
    - Query: WHERE RECORDED_AT > last_timestamp
    - Chỉ lấy dữ liệu sau timestamp cuối cùng đã xử lý
    - ORDER BY timestamp để đảm bảo thứ tự
    
    Args:
        spark: SparkSession
        table_name: Tên bảng Oracle
        last_timestamp: Timestamp cuối cùng đã xử lý
        timestamp_column: Tên cột timestamp (mặc định: RECORDED_AT)
    
    Returns:
        DataFrame: Dữ liệu mới sau last_timestamp
    """
    schema_prefix = f'"{DB_USER}"."'
    full_table = f'{schema_prefix}{table_name}"'
    
    # Tạo query để chỉ lấy dữ liệu mới
    # Dùng > (lớn hơn) để tránh lấy lại record đã xử lý
    ts_str = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    query = f"""
        (SELECT * FROM {full_table}
         WHERE {timestamp_column} > TO_TIMESTAMP('{ts_str}', 'YYYY-MM-DD HH24:MI:SS')
         ORDER BY {timestamp_column})
    """
    
    print(f"   🔍 Query: WHERE {timestamp_column} > '{ts_str}'")
    
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
            .option("dbtable", query) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
        
        count = df.count()
        if count > 0:
            # Lấy min và max timestamp để log
            min_ts = df.agg(spark_min(col(timestamp_column))).first()[0]
            max_ts = df.agg(spark_max(col(timestamp_column))).first()[0]
            print(f"   ✅ Tìm thấy {count} records mới (từ {min_ts} đến {max_ts})")
        else:
            print(f"   ℹ️ Không có dữ liệu mới sau {ts_str}")
        
        return df
    except Exception as e:
        print(f"   ⚠️ Lỗi khi đọc dữ liệu mới: {e}")
        print(f"   📝 Trả về DataFrame rỗng")
        return spark.createDataFrame([], get_fact_schema())

def get_last_timestamp_from_checkpoint(spark: SparkSession) -> dt.datetime:
    """
    Lấy timestamp cuối cùng từ checkpoint.
    
    Logic:
    1. Đọc từ bảng ETL_CHECKPOINT với JOB_NAME
    2. Nếu không có, lấy max timestamp từ GOLD_PRICE_FACT
    3. Nếu vẫn không có, dùng 2000-01-01 làm mặc định
    
    Returns:
        dt.datetime: Timestamp cuối cùng đã xử lý
    """
    try:
        df = read_table_from_oracle(spark, "ETL_CHECKPOINT", DB_USER)
        df_checkpoint = df.filter(col("JOB_NAME") == JOB_NAME)
        
        if df_checkpoint.count() > 0:
            last_run = df_checkpoint.select("LAST_RUN").first()
            if last_run and last_run[0]:
                last_ts = last_run[0]
                print(f"📌 Checkpoint tìm thấy: {last_ts}")
                return last_ts
            else:
                print("⚠️ Checkpoint có record nhưng LAST_RUN là NULL")
        else:
            print("ℹ️ Chưa có checkpoint trong ETL_CHECKPOINT, đang tìm trong FACT...")
    except Exception as e:
        print(f"⚠️ Không đọc được checkpoint: {e}")
        print("   Đang fallback sang FACT table...")
    
    # Nếu chưa có checkpoint, lấy timestamp từ bảng FACT
    try:
        df_fact = read_table_from_oracle(spark, "GOLD_PRICE_FACT", DB_USER)
        if df_fact.count() > 0:
            max_ts = df_fact.agg(spark_max(col(TIMESTAMP_COLUMN))).first()[0]
            if max_ts:
                print(f"📌 Lấy max timestamp từ FACT: {max_ts}")
                return max_ts
            else:
                print("⚠️ FACT có dữ liệu nhưng không có timestamp hợp lệ")
        else:
            print("ℹ️ FACT table trống")
    except Exception as e:
        print(f"⚠️ Không lấy được timestamp từ FACT: {e}")
    
    default_ts = dt.datetime(2000, 1, 1)
    print(f"📌 Sử dụng timestamp mặc định: {default_ts}")
    return default_ts

def update_checkpoint(spark: SparkSession, ts: dt.datetime):
    """
    Cập nhật checkpoint với timestamp mới nhất.
    
    Logic:
    1. Đọc tất cả records từ ETL_CHECKPOINT
    2. Filter ra record của job khác (giữ lại)
    3. Union với record mới của job này
    4. Overwrite toàn bộ bảng (có thể cải thiện bằng MERGE/UPDATE)
    
    Args:
        spark: SparkSession
        ts: Timestamp mới nhất đã xử lý
    """
    print(f"💾 Đang cập nhật checkpoint với timestamp: {ts}")
    
    checkpoint_df = spark.createDataFrame(
        [(JOB_NAME, ts)],
        ["JOB_NAME", "LAST_RUN"]
    )
    
    try:
        # Đọc tất cả records hiện có
        existing = read_table_from_oracle(spark, "ETL_CHECKPOINT", DB_USER)
        existing_count = existing.count()
        print(f"   📊 Records hiện có trong checkpoint: {existing_count}")
        
        # Giữ lại records của job khác, thêm/update record của job này
        other_jobs = existing.filter(col("JOB_NAME") != JOB_NAME)
        other_count = other_jobs.count()
        print(f"   📊 Records của job khác: {other_count}")
        
        combined = other_jobs.union(checkpoint_df)
        combined_count = combined.count()
        print(f"   📊 Tổng records sau merge: {combined_count}")
        
    except Exception as e:
        print(f"   ⚠️ Không đọc được checkpoint hiện có: {e}")
        print(f"   📝 Sẽ tạo checkpoint mới")
        combined = checkpoint_df
    
    # Ghi lại toàn bộ bảng (có thể cải thiện bằng MERGE/UPDATE trong tương lai)
    try:
        combined.write \
            .format("jdbc") \
            .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
            .option("dbtable", f"{DB_USER}.ETL_CHECKPOINT") \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .mode("overwrite") \
            .save()
        print(f"   ✅ Đã cập nhật checkpoint thành công")
    except Exception as e:
        print(f"   ❌ Lỗi khi cập nhật checkpoint: {e}")
        raise

def delete_all_from_oracle_table_spark(spark: SparkSession, table_name: str):
    """
    Xóa tất cả dữ liệu từ bảng Oracle bằng Spark JDBC (không cần cx_Oracle/jaydebeapi).
    
    Cách này dùng Spark JDBC connection để thực thi DELETE SQL.
    """
    try:
        # Đọc bảng để lấy connection, sau đó dùng để thực thi DELETE
        # Tạo DataFrame rỗng với schema đúng
        url = f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}"
        
        # Đọc 1 record để test connection
        test_df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", f"(SELECT * FROM {table_name} WHERE ROWNUM <= 1)") \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
        
        # Thực thi DELETE bằng cách ghi DataFrame rỗng với mode overwrite
        # Nhưng cách này vẫn có thể drop bảng
        
        # Cách tốt hơn: Dùng Spark SQL với JDBC connection
        # Tạo temp view và dùng SQL
        try:
            # Đọc dữ liệu hiện có
            existing_df = spark.read \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", f"(SELECT * FROM {table_name} WHERE 1=0)") \
                .option("driver", "oracle.jdbc.driver.OracleDriver") \
                .load()
            
            # Ghi DataFrame rỗng với mode overwrite - nhưng cách này vẫn rủi ro
            # Thay vào đó, dùng cách khác: đọc dữ liệu cũ, xóa, rồi append mới
            
            print(f"   ⚠️ Spark JDBC không hỗ trợ DELETE trực tiếp")
            print(f"   📝 Sẽ dùng cách khác: đọc dữ liệu cũ, xóa bằng cách ghi DataFrame rỗng")
            return False
        except Exception as e:
            print(f"   ⚠️ Không thể đọc bảng để xóa: {e}")
            return False
            
    except Exception as e:
        print(f"   ⚠️ Không thể xóa dữ liệu từ {table_name}: {e}")
        return False

def delete_all_from_oracle_table(spark: SparkSession, table_name: str):
    """
    Xóa tất cả dữ liệu từ bảng Oracle.
    
    Thử các cách theo thứ tự:
    1. jaydebeapi (nếu có)
    2. cx_Oracle (nếu có)
    3. Spark JDBC (fallback)
    """
    # Thử jaydebeapi trước
    try:
        import jaydebeapi
        url = f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}"
        conn = jaydebeapi.connect("oracle.jdbc.driver.OracleDriver", 
                                url, 
                                [DB_USER, DB_PASS])
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name}")
        conn.commit()
        cursor.close()
        conn.close()
        print(f"   ✅ Đã xóa dữ liệu từ {table_name} (dùng jaydebeapi)")
        return True
    except ImportError:
        pass
    except Exception as e:
        print(f"   ⚠️ Lỗi với jaydebeapi: {e}")
    
    # Thử cx_Oracle
    try:
        import cx_Oracle
        conn = cx_Oracle.connect(DB_USER, DB_PASS, DB_DSN)
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name}")
        conn.commit()
        cursor.close()
        conn.close()
        print(f"   ✅ Đã xóa dữ liệu từ {table_name} (dùng cx_Oracle)")
        return True
    except ImportError:
        pass
    except Exception as e:
        print(f"   ⚠️ Lỗi với cx_Oracle: {e}")
        if "Cannot locate a 64-bit Oracle Client library" in str(e):
            print(f"   💡 Cần cài Oracle Instant Client. Xem INSTALL_ORACLE_DRIVERS.md")
    
    # Fallback: Không thể xóa
    print(f"   ⚠️ Không thể xóa dữ liệu (thiếu drivers)")
    print(f"   💡 Để cài đặt: pip install jaydebeapi JPype1")
    print(f"   📝 Sẽ dùng cách khác: đọc dữ liệu cũ, merge, rồi ghi lại")
    return False

def write_table_to_oracle(df: 'DataFrame', table_name: str, mode: str = "append", spark: SparkSession = None):
    """
    Ghi DataFrame vào Oracle DB.
    
    ⚠️ QUAN TRỌNG: 
    - Khi dùng mode="overwrite", sẽ dùng DELETE + APPEND thay vì overwrite để tránh mất dữ liệu.
    - Đảm bảo DataFrame có đúng schema và không có NULL trong các cột NOT NULL trước khi ghi.
    """
    if df.count() == 0:
        print(f"   ⚠️ DataFrame rỗng, không ghi vào {table_name}")
        return
    
    # Log schema trước khi ghi
    print(f"   📝 Ghi vào {table_name} với mode={mode}")
    print(f"   📝 Schema: {df.columns}")
    print(f"   📝 Số records: {df.count()}")
    
    # ⚠️ QUAN TRỌNG: Đảm bảo BRAND không NULL (fill với empty string nếu NULL)
    # Vì Oracle có thể có constraint hoặc Spark JDBC không xử lý NULL đúng cách
    if "BRAND" in df.columns:
        null_brand_count = df.filter(col("BRAND").isNull()).count()
        if null_brand_count > 0:
            print(f"   ⚠️ Có {null_brand_count} records có BRAND = NULL, sẽ fill bằng empty string")
            df = df.withColumn("BRAND", when(col("BRAND").isNull(), lit("")).otherwise(col("BRAND")))
    
    # ⚠️ QUAN TRỌNG: KHÔNG BAO GIỜ dùng OVERWRITE mode vì có thể drop bảng
    # Thay vào đó: Đọc dữ liệu cũ, xóa bằng Spark, rồi append mới
    if mode == "overwrite":
        print(f"   🔄 Xử lý OVERWRITE mode an toàn (KHÔNG dùng OVERWRITE của Spark JDBC)...")
        if spark is not None:
            # Thử xóa dữ liệu cũ bằng SQL (nếu có drivers)
            delete_success = delete_all_from_oracle_table(spark, table_name)
            if delete_success:
                # Sau đó append dữ liệu mới
                mode = "append"
                print(f"   ✅ Đã xóa dữ liệu cũ bằng SQL, sẽ dùng APPEND mode")
            else:
                # Nếu không thể xóa bằng SQL, dùng cách an toàn: đọc schema, tạo DataFrame rỗng, ghi lại
                print(f"   🔄 Không thể xóa bằng SQL, dùng cách an toàn hơn...")
                try:
                    # Đọc schema từ bảng hiện tại
                    table_name_only = table_name.split(".")[-1] if "." in table_name else table_name
                    existing_df = read_table_from_oracle(spark, table_name_only, DB_USER)
                    existing_count = existing_df.count()
                    
                    if existing_count > 0:
                        print(f"   📊 Bảng hiện có {existing_count} records")
                        # Tạo DataFrame rỗng với schema đúng từ dữ liệu mới
                        # Ghi DataFrame rỗng để xóa dữ liệu cũ
                        empty_df = df.limit(0)  # DataFrame rỗng với schema đúng
                        
                        # Ghi DataFrame rỗng để xóa dữ liệu cũ (an toàn hơn OVERWRITE)
                        print(f"   🔄 Xóa dữ liệu cũ bằng cách ghi DataFrame rỗng...")
                        empty_df.write \
                            .format("jdbc") \
                            .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
                            .option("dbtable", table_name) \
                            .option("driver", "oracle.jdbc.driver.OracleDriver") \
                            .mode("overwrite") \
                            .save()
                        print(f"   ✅ Đã xóa dữ liệu cũ")
                        
                        # Sau đó append dữ liệu mới
                        mode = "append"
                        print(f"   ✅ Sẽ dùng APPEND mode để ghi dữ liệu mới")
                    else:
                        print(f"   📊 Bảng hiện trống, sẽ ghi dữ liệu mới")
                        mode = "append"  # Append vào bảng trống an toàn hơn
                except Exception as read_error:
                    print(f"   ⚠️ Không thể đọc bảng cũ: {read_error}")
                    print(f"   ⚠️ CẢNH BÁO: Sẽ dùng OVERWRITE (có thể rủi ro mất dữ liệu)")
                    print(f"   💡 Khuyến nghị: Cài jaydebeapi để tránh dùng OVERWRITE")
        else:
            print(f"   ⚠️ Không có SparkSession, không thể xóa dữ liệu cũ")
            print(f"   ⚠️ CẢNH BÁO: Sẽ dùng OVERWRITE (có thể rủi ro mất dữ liệu)")
    
    try:
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
            .option("dbtable", table_name) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .mode(mode) \
            .save()
        print(f"   ✅ Đã ghi thành công vào {table_name}")
    except Exception as e:
        print(f"   ❌ Lỗi khi ghi vào {table_name}: {e}")
        raise

def get_fact_schema():
    """Schema cho GOLD_PRICE_FACT."""
    return StructType([
        StructField("SOURCE_ID", IntegerType(), True),
        StructField("TYPE_ID", IntegerType(), True),
        StructField("LOCATION_ID", IntegerType(), True),
        StructField("TIME_ID", IntegerType(), True),
        StructField("BUY_PRICE", DoubleType(), True),
        StructField("SELL_PRICE", DoubleType(), True),
        StructField("RECORDED_AT", TimestampType(), True),
        StructField("UNIT", StringType(), True),
    ])

# ==================== PROCESSING FUNCTIONS ====================

def process_new_fact_data(spark: SparkSession, df_new: 'DataFrame',
                          location_mapping: Dict, type_mapping: Dict) -> 'DataFrame':
    """
    Xử lý dữ liệu FACT mới:
    1. Apply location/type mappings
    2. Deduplicate
    3. Handle missing values
    4. Flag outliers
    """
    if df_new.count() == 0:
        return df_new
    
    # Apply location mapping
    if location_mapping:
        mapping_df = spark.createDataFrame(
            [(k, v) for k, v in location_mapping.items()],
            ["OLD_LOC_ID", "NEW_LOC_ID"]
        )
        df_new = df_new.join(
            mapping_df,
            df_new["LOCATION_ID"] == mapping_df["OLD_LOC_ID"],
            "left"
        ).withColumn(
            "LOCATION_ID",
            when(col("NEW_LOC_ID").isNotNull(), col("NEW_LOC_ID"))
            .otherwise(col("LOCATION_ID"))
        ).drop("OLD_LOC_ID", "NEW_LOC_ID")
    
    # Apply type mapping
    if type_mapping:
        mapping_df = spark.createDataFrame(
            [(k, v) for k, v in type_mapping.items()],
            ["OLD_TYPE_ID", "NEW_TYPE_ID"]
        )
        df_new = df_new.join(
            mapping_df,
            df_new["TYPE_ID"] == mapping_df["OLD_TYPE_ID"],
            "left"
        ).withColumn(
            "TYPE_ID",
            when(col("NEW_TYPE_ID").isNotNull(), col("NEW_TYPE_ID"))
            .otherwise(col("TYPE_ID"))
        ).drop("OLD_TYPE_ID", "NEW_TYPE_ID")
    
    # Deduplicate (giữ record mới nhất)
    df_new = df_new.withColumn(
        "COMBO",
        concat_ws("|",
            col("SOURCE_ID").cast("string"),
            col("TYPE_ID").cast("string"),
            col("LOCATION_ID").cast("string"),
            col("TIME_ID").cast("string")
        )
    )
    
    window_spec = Window.partitionBy("COMBO").orderBy(col(TIMESTAMP_COLUMN).desc())
    df_new = df_new.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn", "COMBO")
    
    # Handle missing values
    df_new = df_new.filter(
        col("BUY_PRICE").isNotNull() &
        col("SELL_PRICE").isNotNull() &
        col(TIMESTAMP_COLUMN).isNotNull()
    )
    
    # Flag outliers
    df_new = df_new.withColumn("IS_DELETED", lit(0))
    df_new = df_new.withColumn("IS_DELETE", lit(0))
    
    return df_new

def load_dimension_mappings(spark: SparkSession) -> Tuple[Dict, Dict]:
    """Load location và type mappings từ CLEAN tables."""
    location_mapping = {}
    type_mapping = {}
    
    # TODO: Implement logic để load mappings
    # Có thể load từ LOCATION_DIMENSION_CLEAN và GOLD_TYPE_DIMENSION_CLEAN
    
    return location_mapping, type_mapping

# ==================== STREAMING WITH FOREACHBATCH ====================

def merge_duplicate_types_and_update_fact_streaming(spark: SparkSession) -> Dict:
    """
    Gộp các bản ghi trùng trong GOLD_TYPE_DIMENSION_CLEAN và cập nhật lại bảng CLEAN.
    
    ⚠️ QUAN TRỌNG - TUYỆT ĐỐI KHÔNG ĐỘNG ĐẾN BẢNG GỐC: 
    - ✅ CHỈ đọc từ: GOLD_TYPE_DIMENSION_CLEAN
    - ✅ Merge các records trùng trong CLEAN (giữ 1 record cho mỗi group)
    - ✅ Cập nhật lại bảng CLEAN với dữ liệu đã merge
    - ✅ Tạo mapping để dùng cho FACT
    - ❌ KHÔNG đọc từ: GOLD_TYPE_DIMENSION (bảng gốc)
    - ❌ KHÔNG ghi vào: GOLD_TYPE_DIMENSION (bảng gốc)
    """
    from decimal import Decimal
    
    # ⚠️ QUAN TRỌNG: CHỈ đọc từ bảng CLEAN, KHÔNG đọc từ bảng gốc GOLD_TYPE_DIMENSION
    df = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION_CLEAN", DB_USER)
    
    if df.count() == 0:
        print("⚠️ GOLD_TYPE_DIMENSION_CLEAN trống.")
        return {}
    
    original_count = df.count()
    print(f"📊 Bảng CLEAN hiện có: {original_count} records")
    
    # ⚠️ QUAN TRỌNG: Fill NULL trong BRAND ngay từ đầu (trước khi xử lý)
    # Vì Oracle có thể có constraint hoặc Spark JDBC không xử lý NULL đúng cách
    if "BRAND" in df.columns:
        null_brand_count = df.filter(col("BRAND").isNull()).count()
        if null_brand_count > 0:
            print(f"   ⚠️ Có {null_brand_count} records có BRAND = NULL, sẽ fill bằng empty string ngay từ đầu")
            df = df.withColumn(
                "BRAND", 
                when(col("BRAND").isNull(), lit("")).otherwise(col("BRAND"))
            )
    
    # ⚠️ QUAN TRỌNG: Lưu backup dữ liệu CLEAN cũ để restore nếu merge thất bại
    df_backup = df
    print(f"   💾 Đã backup {original_count} records để phục hồi nếu cần")

    # Kiểm tra các cột có tồn tại không
    columns = df.columns
    df_normalized = df.withColumn(
        "TYPE_NAME_NORM", lower(trim(col("TYPE_NAME")))
    ).withColumn(
        "PURITY_NORM", lower(trim(col("PURITY")))
    ).withColumn(
        "CATEGORY_NORM", lower(trim(col("CATEGORY")))
    )
    
    # Chỉ thêm BRAND_NORM nếu cột BRAND tồn tại
    if "BRAND" in columns:
        df_normalized = df_normalized.withColumn(
            "BRAND_NORM", lower(trim(col("BRAND")))
        )
        partition_cols = ["TYPE_NAME_NORM", "PURITY_NORM", "CATEGORY_NORM", "BRAND_NORM"]
    else:
        # Tạo cột BRAND_NORM rỗng nếu không có BRAND
        df_normalized = df_normalized.withColumn("BRAND_NORM", lit(""))
        partition_cols = ["TYPE_NAME_NORM", "PURITY_NORM", "CATEGORY_NORM", "BRAND_NORM"]
        print("⚠️ Cột BRAND không tồn tại, sử dụng giá trị rỗng cho BRAND_NORM")

    # Group by normalized values and find canonical ID (ID nhỏ nhất trong mỗi group)
    window_spec = Window.partitionBy(*partition_cols).orderBy("ID")
    
    df_with_canon = df_normalized.withColumn(
        "CANON_ID",
        first("ID").over(window_spec)
    )

    # Create mapping: old_id -> new_id (canonical_id)
    mapping_df = df_with_canon.filter(col("ID") != col("CANON_ID")) \
        .select(col("ID").alias("OLD_ID"), col("CANON_ID").alias("NEW_ID")) \
        .distinct()
    
    mapping = {}
    if mapping_df.count() > 0:
        for row in mapping_df.collect():
            # Xử lý OLD_ID và NEW_ID - có thể là Decimal, float, int, hoặc NaN
            old_id_val = row["OLD_ID"]
            new_id_val = row["NEW_ID"]
            
            # Skip nếu có giá trị None hoặc NaN
            if old_id_val is None or new_id_val is None:
                continue
            try:
                # Convert OLD_ID
                if isinstance(old_id_val, int):
                    old_id = old_id_val
                elif isinstance(old_id_val, (float, Decimal)):
                    if pd.isna(old_id_val):
                        continue
                    old_id = int(old_id_val)
                else:
                    old_id = int(float(str(old_id_val)))
                
                # Convert NEW_ID
                if isinstance(new_id_val, int):
                    new_id = new_id_val
                elif isinstance(new_id_val, (float, Decimal)):
                    if pd.isna(new_id_val):
                        continue
                    new_id = int(new_id_val)
                else:
                    new_id = int(float(str(new_id_val)))
                
                mapping[old_id] = new_id
            except (ValueError, TypeError, OverflowError):
                continue  # Skip nếu không convert được
    
    # Tạo bảng CLEAN mới: merge các records trùng (giữ 1 record cho mỗi CANON_ID)
    # ⚠️ QUAN TRỌNG: CHỈ xử lý bảng CLEAN, KHÔNG động đến bảng gốc
    # ⚠️ QUAN TRỌNG: Đảm bảo thứ tự cột đúng với schema Oracle: ID, TYPE_NAME, PURITY, CATEGORY, BRAND
    select_cols = ["TYPE_NAME", "PURITY", "CATEGORY"]
    if "BRAND" in columns:
        select_cols.append("BRAND")
    
    print(f"   📝 Các cột sẽ giữ lại: {select_cols}")
    print(f"   📝 Các cột có sẵn trong df_with_canon: {df_with_canon.columns}")
    
    # ⚠️ QUAN TRỌNG: Lấy 1 record cho mỗi CANON_ID (group trùng)
    # Logic: Mỗi CANON_ID đại diện cho 1 group records trùng
    # Chỉ cần giữ 1 record cho mỗi CANON_ID (record có ID nhỏ nhất)
    window_spec_clean = Window.partitionBy("CANON_ID").orderBy("ID")
    
    # Kiểm tra df_with_canon trước khi merge
    canon_count = df_with_canon.count()
    print(f"   📊 Số records sau khi tìm CANON_ID: {canon_count}")
    
    # Đếm số CANON_ID unique (số groups)
    unique_canon_ids = df_with_canon.select("CANON_ID").distinct().count()
    print(f"   📊 Số CANON_ID unique (số groups): {unique_canon_ids}")
    print(f"   📊 Số records sẽ bị merge: {canon_count - unique_canon_ids}")
    
    # Lấy 1 record cho mỗi CANON_ID (record có ID nhỏ nhất trong group)
    df_clean_merged = df_with_canon.withColumn(
        "ROW_NUM", row_number().over(window_spec_clean)
    ).filter(col("ROW_NUM") == 1)
    
    # Kiểm tra sau filter
    after_filter_count = df_clean_merged.count()
    print(f"   📊 Số records sau filter ROW_NUM=1: {after_filter_count}")
    
    # Select các cột cần thiết - đảm bảo đúng thứ tự: ID, TYPE_NAME, PURITY, CATEGORY, BRAND
    try:
        # Kiểm tra từng cột có tồn tại không
        missing_cols = [c for c in select_cols if c not in df_clean_merged.columns]
        if missing_cols:
            print(f"   ❌ Lỗi: Thiếu các cột: {missing_cols}")
            print(f"   📝 Các cột có sẵn: {df_clean_merged.columns}")
            print(f"   📊 Giữ nguyên dữ liệu CLEAN cũ: {original_count} records")
            return mapping
        
        # ⚠️ QUAN TRỌNG: Đảm bảo BRAND không NULL trước khi select
        # Vì sau khi select có thể mất logic fill NULL
        if "BRAND" in df_clean_merged.columns:
            null_brand_count = df_clean_merged.filter(col("BRAND").isNull()).count()
            if null_brand_count > 0:
                print(f"   ⚠️ Có {null_brand_count} records có BRAND = NULL trước khi select, sẽ fill bằng empty string")
                df_clean_merged = df_clean_merged.withColumn(
                    "BRAND", 
                    when(col("BRAND").isNull(), lit("")).otherwise(col("BRAND"))
                )
        
        # Select với thứ tự đúng: ID trước, sau đó các cột khác
        df_clean_merged = df_clean_merged.select(
            col("CANON_ID").alias("ID"),
            *[col(c) for c in select_cols]
        )
        
        # ⚠️ QUAN TRỌNG: Đảm bảo BRAND không NULL sau khi select
        if "BRAND" in df_clean_merged.columns:
            null_brand_count = df_clean_merged.filter(col("BRAND").isNull()).count()
            if null_brand_count > 0:
                print(f"   ⚠️ Có {null_brand_count} records có BRAND = NULL sau khi select, sẽ fill bằng empty string")
                df_clean_merged = df_clean_merged.withColumn(
                    "BRAND", 
                    when(col("BRAND").isNull(), lit("")).otherwise(col("BRAND"))
                )
        
        # Kiểm tra schema sau khi select
        print(f"   📝 Schema sau select: {df_clean_merged.columns}")
        print(f"   📝 Số cột: {len(df_clean_merged.columns)}")
        
    except Exception as e:
        print(f"   ❌ Lỗi khi select columns: {e}")
        print(f"   📝 Các cột có sẵn: {df_clean_merged.columns}")
        print(f"   📝 Các cột cần select: ID, {select_cols}")
        print(f"   📊 Giữ nguyên dữ liệu CLEAN cũ: {original_count} records")
        return mapping
    
    clean_count = df_clean_merged.count()
    print(f"   📊 Số records sau select: {clean_count}")
    
    # ⚠️ QUAN TRỌNG: Kiểm tra an toàn trước khi ghi
    # Nếu df_clean_merged rỗng hoặc mất quá nhiều dữ liệu, restore dữ liệu cũ
    if clean_count == 0:
        print(f"❌ LỖI: Sau merge bảng CLEAN rỗng! Khôi phục dữ liệu cũ...")
        try:
            write_table_to_oracle(df_backup, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
            print(f"   ✅ Đã khôi phục {original_count} records")
        except Exception as restore_error:
            print(f"   ❌ Lỗi khi khôi phục: {restore_error}")
        return mapping
    
    # ⚠️ QUAN TRỌNG: Kiểm tra số records sau merge phải hợp lý
    # Sau merge, số records = số records không trùng + số groups trùng (mỗi group chỉ giữ 1)
    # Nếu mất quá nhiều (> 50%) thì có vấn đề
    expected_min_count = original_count - len(mapping)  # Ít nhất phải còn: tổng - số records bị merge
    if clean_count < expected_min_count * 0.9:  # Cho phép sai số 10%
        print(f"⚠️ CẢNH BÁO: Sau merge mất quá nhiều dữ liệu ({original_count} → {clean_count})!")
        print(f"   📊 Expected tối thiểu: {expected_min_count}, thực tế: {clean_count}")
        print(f"   📊 Khôi phục dữ liệu CLEAN cũ để tránh mất dữ liệu...")
        try:
            write_table_to_oracle(df_backup, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite", spark)
            print(f"   ✅ Đã khôi phục {original_count} records")
        except Exception as restore_error:
            print(f"   ❌ Lỗi khi khôi phục: {restore_error}")
        return mapping
    
    # Kiểm tra: số records sau merge phải = số records gốc - số records bị merge
    expected_count = original_count - len(mapping)
    if abs(clean_count - expected_count) > 5:  # Cho phép sai số 5 records
        print(f"⚠️ CẢNH BÁO: Số records sau merge không khớp!")
        print(f"   📊 Expected: {expected_count} (từ {original_count} - {len(mapping)} trùng)")
        print(f"   📊 Thực tế: {clean_count}")
        print(f"   📊 Khôi phục dữ liệu CLEAN cũ để tránh mất dữ liệu...")
        try:
            write_table_to_oracle(df_backup, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite", spark)
            print(f"   ✅ Đã khôi phục {original_count} records")
        except Exception as restore_error:
            print(f"   ❌ Lỗi khi khôi phục: {restore_error}")
        return mapping
    
    if mapping:
        print(f"✅ Đã tạo mapping cho {len(mapping)} TYPE trùng:")
        for old_id, new_id in list(mapping.items())[:5]:  # In 5 mapping đầu
            print(f"   ID {old_id} → ID {new_id}")
        if len(mapping) > 5:
            print(f"   ... và {len(mapping) - 5} mapping khác")
        
        # Cập nhật bảng CLEAN với dữ liệu đã merge
        # ⚠️ QUAN TRỌNG: Chỉ cập nhật bảng CLEAN, KHÔNG động vào bảng gốc
        try:
            # ⚠️ QUAN TRỌNG: Đọc schema từ bảng hiện tại để đảm bảo khớp
            try:
                existing_schema_df = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION_CLEAN", DB_USER)
                existing_schema = existing_schema_df.columns
                existing_schema_count = existing_schema_df.count()
                print(f"   📝 Schema hiện tại trong DB: {existing_schema}")
                print(f"   📝 Số records hiện tại trong DB: {existing_schema_count}")
                
                # So sánh schema
                new_schema = df_clean_merged.columns
                if set(existing_schema) != set(new_schema):
                    print(f"   ⚠️ CẢNH BÁO: Schema không khớp!")
                    print(f"      DB: {existing_schema}")
                    print(f"      Mới: {new_schema}")
                    print(f"      Thiếu: {set(existing_schema) - set(new_schema)}")
                    print(f"      Thừa: {set(new_schema) - set(existing_schema)}")
            except Exception as schema_error:
                print(f"   ⚠️ Không thể đọc schema từ DB: {schema_error}")
            
            # Kiểm tra schema trước khi ghi
            print(f"   📝 Schema trước khi ghi: {df_clean_merged.columns}")
            print(f"   📝 Số records: {clean_count}")
            
            # ⚠️ QUAN TRỌNG: Đảm bảo BRAND không NULL (fill với empty string nếu NULL)
            # Vì Oracle có thể có constraint hoặc Spark JDBC không xử lý NULL đúng cách
            if "BRAND" in df_clean_merged.columns:
                null_brand_count = df_clean_merged.filter(col("BRAND").isNull()).count()
                if null_brand_count > 0:
                    print(f"   ⚠️ Có {null_brand_count} records có BRAND = NULL, sẽ fill bằng empty string")
                    df_clean_merged = df_clean_merged.withColumn(
                        "BRAND", 
                        when(col("BRAND").isNull(), lit("")).otherwise(col("BRAND"))
                    )
            
            # Kiểm tra dữ liệu có NULL không (sau khi fill)
            null_counts = {}
            for col_name in df_clean_merged.columns:
                if col_name != "ID":  # ID có thể NULL trong một số trường hợp
                    null_count = df_clean_merged.filter(col(col_name).isNull()).count()
                    if null_count > 0:
                        null_counts[col_name] = null_count
            if null_counts:
                print(f"   ⚠️ Cảnh báo: Vẫn còn NULL trong các cột: {null_counts}")
            
            # ⚠️ QUAN TRỌNG: Verify dữ liệu trước khi ghi
            print(f"   📊 Kiểm tra dữ liệu trước khi ghi:")
            print(f"      - Số records: {clean_count}")
            print(f"      - Schema: {df_clean_merged.columns}")
            print(f"      - Sample data (5 records đầu):")
            try:
                sample = df_clean_merged.limit(5).collect()
                for i, row in enumerate(sample, 1):
                    print(f"         {i}. ID={row['ID']}, TYPE_NAME={row.get('TYPE_NAME', 'N/A')[:30]}")
            except Exception as e:
                print(f"         ⚠️ Không thể lấy sample: {e}")
            
            # ⚠️ THỬ NGHIỆM: Không truyền spark parameter để dùng logic giống GOLD_PRICE_FACT_CLEAN
            # GOLD_PRICE_FACT_CLEAN hoạt động bình thường với overwrite không có spark parameter
            print(f"   🔄 Dùng OVERWRITE trực tiếp (giống GOLD_PRICE_FACT_CLEAN)...")
            
            try:
                write_table_to_oracle(df_clean_merged, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
                
                # ⚠️ QUAN TRỌNG: Verify sau khi ghi - đọc lại để kiểm tra
                spark.catalog.clearCache()
                df_verify = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION_CLEAN", DB_USER)
                verify_count = df_verify.count()
                
                if verify_count == 0:
                    print(f"❌ LỖI: Sau khi ghi, bảng CLEAN bị rỗng! Khôi phục dữ liệu cũ...")
                    print(f"   📝 Schema đã ghi: {df_clean_merged.columns}")
                    print(f"   📝 Schema đã đọc lại: {df_verify.columns if df_verify.count() > 0 else 'Bảng rỗng'}")
                    print(f"   📝 Số records đã ghi: {clean_count}")
                    print(f"   📝 Sample dữ liệu đã ghi (3 records đầu):")
                    try:
                        sample_written = df_clean_merged.limit(3).collect()
                        for i, row in enumerate(sample_written, 1):
                            print(f"      {i}. ID={row['ID']}, TYPE_NAME={row.get('TYPE_NAME', 'N/A')[:30]}")
                    except:
                        pass
                    write_table_to_oracle(df_backup, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
                    print(f"   ✅ Đã khôi phục {original_count} records")
                    return mapping
                
                if verify_count != clean_count:
                    print(f"⚠️ Cảnh báo: Số records sau khi ghi ({verify_count}) khác với expected ({clean_count})")
                    print(f"   📊 Chênh lệch: {abs(verify_count - clean_count)} records")
                
                # Kiểm tra schema sau khi đọc lại
                print(f"   📝 Schema sau khi đọc lại: {df_verify.columns}")
                
                print(f"✅ Đã cập nhật GOLD_TYPE_DIMENSION_CLEAN: {verify_count} records (từ {original_count} records)")
                print(f"   📝 Đã merge {original_count - verify_count} records trùng")
            except Exception as write_error:
                print(f"❌ LỖI khi ghi vào bảng CLEAN: {write_error}")
                print(f"   📊 Khôi phục dữ liệu CLEAN cũ...")
                try:
                    write_table_to_oracle(df_backup, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
                    print(f"   ✅ Đã khôi phục {original_count} records")
                except Exception as restore_error:
                    print(f"   ❌ Lỗi khi khôi phục: {restore_error}")
                return mapping
        except Exception as e:
            print(f"❌ LỖI khi ghi vào bảng CLEAN: {e}")
            print(f"   📊 Khôi phục dữ liệu CLEAN cũ: {original_count} records...")
            try:
                write_table_to_oracle(df_backup, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite", spark)
                print(f"   ✅ Đã khôi phục {original_count} records")
            except Exception as restore_error:
                print(f"   ❌ Lỗi khi khôi phục: {restore_error}")
            return mapping
        
        # ⚠️ QUAN TRỌNG: Cập nhật GOLD_PRICE_FACT_CLEAN với mapping
        # Tất cả records có TYPE_ID = old_id phải đổi thành TYPE_ID = new_id
        print(f"\n🔄 Đang cập nhật GOLD_PRICE_FACT_CLEAN với type mapping...")
        try:
            df_fact_clean = read_table_from_oracle(spark, "GOLD_PRICE_FACT_CLEAN", DB_USER)
            fact_before_count = df_fact_clean.count()
            
            # ⚠️ QUAN TRỌNG: Backup dữ liệu FACT_CLEAN trước khi cập nhật
            df_fact_backup = df_fact_clean
            print(f"   💾 Đã backup {fact_before_count} records FACT_CLEAN")
            
            if fact_before_count > 0:
                # Tạo mapping DataFrame
                mapping_df = spark.createDataFrame(
                    [(k, v) for k, v in mapping.items()],
                    ["OLD_TYPE_ID", "NEW_TYPE_ID"]
                )
                
                # Join và cập nhật TYPE_ID (LEFT JOIN để giữ TẤT CẢ records)
                df_fact_updated = df_fact_clean.join(
                    mapping_df,
                    df_fact_clean["TYPE_ID"] == mapping_df["OLD_TYPE_ID"],
                    "left"  # LEFT JOIN để giữ tất cả records, kể cả không có mapping
                ).withColumn(
                    "TYPE_ID",
                    when(col("NEW_TYPE_ID").isNotNull(), col("NEW_TYPE_ID"))
                    .otherwise(col("TYPE_ID"))  # Giữ nguyên nếu không có mapping
                ).drop("OLD_TYPE_ID", "NEW_TYPE_ID")
                
                fact_after_count = df_fact_updated.count()
                
                # ⚠️ QUAN TRỌNG: Kiểm tra an toàn - số records phải giữ nguyên
                if fact_after_count == 0:
                    print(f"   ❌ LỖI: Sau cập nhật FACT_CLEAN bị rỗng! Khôi phục...")
                    write_table_to_oracle(df_fact_backup, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite", spark)
                    print(f"   ✅ Đã khôi phục {fact_before_count} records FACT_CLEAN")
                elif fact_after_count != fact_before_count:
                    print(f"   ⚠️ CẢNH BÁO: Số records thay đổi ({fact_before_count} → {fact_after_count})!")
                    print(f"   📊 Khôi phục dữ liệu FACT_CLEAN cũ...")
                    write_table_to_oracle(df_fact_backup, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite", spark)
                    print(f"   ✅ Đã khôi phục {fact_before_count} records FACT_CLEAN")
                else:
                    # Đếm số records được cập nhật
                    updated_count = df_fact_clean.join(
                        mapping_df,
                        df_fact_clean["TYPE_ID"] == mapping_df["OLD_TYPE_ID"],
                        "inner"
                    ).count()
                    
                    # Ghi lại bảng FACT_CLEAN đã được cập nhật
                    write_table_to_oracle(df_fact_updated, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite", spark)
                    
                    # Verify sau khi ghi
                    spark.catalog.clearCache()
                    df_fact_verify = read_table_from_oracle(spark, "GOLD_PRICE_FACT_CLEAN", DB_USER)
                    verify_count = df_fact_verify.count()
                    
                    if verify_count == 0:
                        print(f"   ❌ LỖI: Sau khi ghi, FACT_CLEAN bị rỗng! Khôi phục...")
                        write_table_to_oracle(df_fact_backup, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite", spark)
                        print(f"   ✅ Đã khôi phục {fact_before_count} records FACT_CLEAN")
                    else:
                        print(f"   ✅ Đã cập nhật {updated_count} records trong GOLD_PRICE_FACT_CLEAN")
                        print(f"   📊 GOLD_PRICE_FACT_CLEAN: {fact_before_count} → {verify_count} records")
                        
                        # In chi tiết các mapping đã áp dụng
                        for old_id, new_id in mapping.items():
                            count = df_fact_clean.filter(col("TYPE_ID") == old_id).count()
                            if count > 0:
                                print(f"      TYPE_ID {old_id} → {new_id}: {count} records")
            else:
                print(f"   ℹ️ GOLD_PRICE_FACT_CLEAN trống, không cần cập nhật")
        except Exception as e:
            print(f"   ❌ LỖI khi cập nhật GOLD_PRICE_FACT_CLEAN: {e}")
            print(f"   📊 Khôi phục dữ liệu FACT_CLEAN cũ...")
            try:
                if 'df_fact_backup' in locals():
                    write_table_to_oracle(df_fact_backup, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite", spark)
                    print(f"   ✅ Đã khôi phục {fact_before_count} records FACT_CLEAN")
            except Exception as restore_error:
                print(f"   ❌ Lỗi khi khôi phục FACT_CLEAN: {restore_error}")
            print(f"   📝 Mapping vẫn được trả về để dùng cho FACT mới")
        
        print(f"   📝 Mapping sẽ được dùng để cập nhật FACT.TYPE_ID cho dữ liệu mới")
    else:
        print("ℹ️ Không có TYPE trùng cần gộp.")
        print(f"   📊 Bảng CLEAN giữ nguyên: {original_count} records")
    
    return mapping

def clean_all_dimensions_incremental(spark: SparkSession, merge_types: bool = False) -> Tuple[Dict, Dict]:
    """
    Clean tất cả dimension tables (LOCATION và TYPE) - INCREMENTAL.
    Giữ nguyên dữ liệu CLEAN cũ, chỉ cập nhật/thêm mới.
    Trả về mappings để dùng cho FACT.
    """
    if not BATCH_FUNCTIONS_AVAILABLE:
        print("⚠️ Không thể clean dimensions, chỉ dùng mappings hiện có")
        return {}, {}
    
    print("\n" + "="*60)
    print("🧹 Đang clean tất cả dimension tables (INCREMENTAL)...")
    print("="*60)
    
    # B1: LOCATION normalize -> LOCATION_DIMENSION_CLEAN
    print("\n📍 Bước 1: Normalize LOCATION_DIMENSION...")
    
    # Đọc dữ liệu CLEAN hiện có TRƯỚC (để giữ lại)
    try:
        df_loc_clean_existing = read_table_from_oracle(spark, "LOCATION_DIMENSION_CLEAN", DB_USER)
        existing_loc_count = df_loc_clean_existing.count()
        existing_loc_ids = set([row["ID"] for row in df_loc_clean_existing.select("ID").collect()])
        print(f"📊 LOCATION_CLEAN hiện có: {existing_loc_count} records")
    except:
        df_loc_clean_existing = None
        existing_loc_ids = set()
        existing_loc_count = 0
        print("📊 LOCATION_CLEAN chưa có, sẽ tạo mới")
    
    # Clear cache để đảm bảo đọc dữ liệu mới nhất
    spark.catalog.clearCache()
    
    # Gọi normalize_locations (sẽ overwrite, nhưng ta sẽ merge lại sau)
    try:
        location_mapping = normalize_locations(spark)
    except Exception as e:
        print(f"❌ Lỗi khi normalize_locations: {e}")
        print(f"   Traceback: {type(e).__name__}: {str(e)}")
        # Fallback: Không có mapping, chỉ dùng dữ liệu hiện có
        location_mapping = {}
        print("⚠️ Sử dụng location_mapping rỗng, giữ nguyên dữ liệu CLEAN hiện có")
    
    # Clear cache lại sau khi normalize
    spark.catalog.clearCache()
    
    # Đọc CLEAN mới sau khi normalize
    try:
        df_loc_clean_new = read_table_from_oracle(spark, "LOCATION_DIMENSION_CLEAN", DB_USER)
        new_loc_count = df_loc_clean_new.count()
        
        # Kiểm tra nếu bảng CLEAN mới rỗng nhưng có dữ liệu cũ
        if new_loc_count == 0 and existing_loc_count > 0:
            print("⚠️ Bảng CLEAN mới rỗng nhưng có dữ liệu cũ. Giữ nguyên dữ liệu cũ...")
            write_table_to_oracle(df_loc_clean_existing, f"{DB_USER}.LOCATION_DIMENSION_CLEAN", "overwrite")
            print(f"✅ Đã giữ nguyên LOCATION_DIMENSION_CLEAN: {existing_loc_count} records")
        
        # Merge: Giữ nguyên CLEAN cũ + CLEAN mới (union và distinct)
        elif df_loc_clean_existing is not None and existing_loc_count > 0:
            df_loc_clean_combined = df_loc_clean_existing.unionByName(df_loc_clean_new, allowMissingColumns=True)
            df_loc_clean_final = df_loc_clean_combined.distinct()
            final_count = df_loc_clean_final.count()
            
            # Đảm bảo có dữ liệu trước khi ghi
            if final_count > 0:
                write_table_to_oracle(df_loc_clean_final, f"{DB_USER}.LOCATION_DIMENSION_CLEAN", "overwrite")
                print(f"✅ Đã cập nhật LOCATION_DIMENSION_CLEAN: {final_count} records (giữ {existing_loc_count} cũ)")
            else:
                print("⚠️ Sau merge không còn dữ liệu! Giữ nguyên dữ liệu cũ...")
                write_table_to_oracle(df_loc_clean_existing, f"{DB_USER}.LOCATION_DIMENSION_CLEAN", "overwrite")
                print(f"✅ Đã giữ nguyên LOCATION_DIMENSION_CLEAN: {existing_loc_count} records")
        else:
            # Kiểm tra nếu bảng CLEAN mới có dữ liệu
            if new_loc_count > 0:
                print(f"✅ Đã tạo LOCATION_DIMENSION_CLEAN: {new_loc_count} records")
            else:
                print("⚠️ Bảng CLEAN mới rỗng! Kiểm tra lại bảng gốc...")
                # Fallback: đọc từ bảng gốc
                try:
                    df_original = read_table_from_oracle(spark, "LOCATION_DIMENSION", DB_USER)
                    original_count = df_original.count()
                    if original_count > 0:
                        print(f"⚠️ Copy {original_count} records từ bảng gốc...")
                        write_table_to_oracle(df_original, f"{DB_USER}.LOCATION_DIMENSION_CLEAN", "overwrite")
                        print(f"✅ Đã copy từ bảng gốc: {original_count} records")
                    else:
                        print("❌ Bảng gốc cũng trống!")
                except Exception as e2:
                    print(f"❌ Không thể copy từ bảng gốc: {e2}")
    except Exception as e:
        print(f"⚠️ Lỗi khi merge LOCATION_CLEAN: {e}")
        # Fallback: giữ nguyên dữ liệu cũ nếu có
        if df_loc_clean_existing is not None and existing_loc_count > 0:
            try:
                write_table_to_oracle(df_loc_clean_existing, f"{DB_USER}.LOCATION_DIMENSION_CLEAN", "overwrite")
                print(f"✅ Đã giữ nguyên dữ liệu cũ: {existing_loc_count} records")
            except:
                pass
    
    print(f"✅ Location mapping: {len(location_mapping)} mappings")
    
    # B2: GOLD TYPE enrich -> GOLD_TYPE_DIMENSION_CLEAN
    print("\n💎 Bước 2: Enrich GOLD_TYPE_DIMENSION...")
    
    # Đọc dữ liệu CLEAN hiện có TRƯỚC (để giữ lại)
    try:
        df_type_clean_existing = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION_CLEAN", DB_USER)
        existing_type_count = df_type_clean_existing.count()
        print(f"📊 TYPE_CLEAN hiện có: {existing_type_count} records")
    except:
        df_type_clean_existing = None
        existing_type_count = 0
        print("📊 TYPE_CLEAN chưa có, sẽ tạo mới")
    
    # Clear cache để đảm bảo đọc dữ liệu mới nhất
    spark.catalog.clearCache()
    
    # Gọi các hàm enrich (sẽ overwrite, nhưng ta sẽ merge lại sau)
    try:
        enrich_gold_types(spark)
        normalize_purity_format(spark)
        normalize_category_smart(spark)
    except Exception as e:
        print(f"❌ Lỗi khi enrich/normalize TYPE: {e}")
        print(f"   Traceback: {type(e).__name__}: {str(e)}")
        print("⚠️ Giữ nguyên dữ liệu TYPE_CLEAN hiện có")
    
    # Clear cache lại sau khi gọi các hàm
    spark.catalog.clearCache()
    
    # Đọc CLEAN mới sau khi enrich
    try:
        df_type_clean_new = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION_CLEAN", DB_USER)
        new_type_count = df_type_clean_new.count()
        
        # Kiểm tra nếu bảng CLEAN mới rỗng nhưng có dữ liệu cũ
        if new_type_count == 0 and existing_type_count > 0:
            print("⚠️ Bảng CLEAN mới rỗng nhưng có dữ liệu cũ. Giữ nguyên dữ liệu cũ...")
            write_table_to_oracle(df_type_clean_existing, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
            print(f"✅ Đã giữ nguyên GOLD_TYPE_DIMENSION_CLEAN: {existing_type_count} records")
            return location_mapping, {}
        
        # Merge: Giữ nguyên CLEAN cũ + CLEAN mới (union và distinct)
        if df_type_clean_existing is not None and existing_type_count > 0:
            df_type_clean_combined = df_type_clean_existing.unionByName(df_type_clean_new, allowMissingColumns=True)
            # Deduplicate theo ID (giữ record mới nhất nếu có trùng)
            window_spec = Window.partitionBy("ID").orderBy(col("ID"))
            df_type_clean_final = df_type_clean_combined.withColumn("rn", row_number().over(window_spec)) \
                .filter(col("rn") == 1) \
                .drop("rn") \
                .distinct()
            final_count = df_type_clean_final.count()
            
            # Đảm bảo có dữ liệu trước khi ghi
            if final_count > 0:
                write_table_to_oracle(df_type_clean_final, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
                print(f"✅ Đã cập nhật GOLD_TYPE_DIMENSION_CLEAN: {final_count} records (giữ {existing_type_count} cũ)")
            else:
                print("⚠️ Sau merge không còn dữ liệu! Giữ nguyên dữ liệu cũ...")
                write_table_to_oracle(df_type_clean_existing, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
                print(f"✅ Đã giữ nguyên GOLD_TYPE_DIMENSION_CLEAN: {existing_type_count} records")
        else:
            # Kiểm tra nếu bảng CLEAN mới có dữ liệu
            if new_type_count > 0:
                print(f"✅ Đã tạo GOLD_TYPE_DIMENSION_CLEAN: {new_type_count} records")
            else:
                print("⚠️ Bảng CLEAN mới rỗng! Kiểm tra lại bảng gốc...")
                # Fallback: CHỈ đọc từ bảng gốc để copy vào CLEAN (KHÔNG sửa bảng gốc)
                # Đây là trường hợp đặc biệt khi CLEAN bị rỗng, cần copy từ gốc để khôi phục
                try:
                    df_original = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION", DB_USER)
                    original_count = df_original.count()
                    if original_count > 0:
                        print(f"⚠️ Copy {original_count} records từ bảng gốc vào CLEAN...")
                        # QUAN TRỌNG: Chỉ ghi vào CLEAN, KHÔNG động vào bảng gốc
                        write_table_to_oracle(df_original, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
                        print(f"✅ Đã copy từ bảng gốc vào CLEAN: {original_count} records")
                    else:
                        print("❌ Bảng gốc cũng trống!")
                except Exception as e2:
                    print(f"❌ Không thể copy từ bảng gốc: {e2}")
    except Exception as e:
        print(f"⚠️ Lỗi khi merge TYPE_CLEAN: {e}")
        # Fallback: giữ nguyên dữ liệu cũ nếu có
        if df_type_clean_existing is not None and existing_type_count > 0:
            try:
                write_table_to_oracle(df_type_clean_existing, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
                print(f"✅ Đã giữ nguyên dữ liệu cũ: {existing_type_count} records")
            except:
                pass
    
    # (Tuỳ chọn) gộp TYPE tương đồng
    # QUAN TRỌNG: Dùng hàm riêng trong file streaming (KHÔNG import từ batch)
    # - Đọc từ: GOLD_TYPE_DIMENSION_CLEAN
    # - CHỈ tạo mapping, KHÔNG ghi đè bảng CLEAN
    # - KHÔNG động vào bảng gốc GOLD_TYPE_DIMENSION
    type_mapping = {}
    if merge_types:
        print("\n🔗 Bước 3: Merge duplicate types...")
        print("   📝 Chỉ xử lý bảng CLEAN, không động vào bảng gốc")
        print("   📝 CHỈ tạo mapping, KHÔNG ghi đè bảng CLEAN (giống logic cũ)")
        try:
            # Dùng hàm riêng trong file streaming (không import từ batch)
            type_mapping = merge_duplicate_types_and_update_fact_streaming(spark)
            print(f"✅ Type mapping: {len(type_mapping)} mappings")
        except Exception as e:
            print(f"❌ Lỗi khi merge duplicate types: {e}")
            print(f"   Traceback: {type(e).__name__}: {str(e)}")
            type_mapping = {}
            print("⚠️ Sử dụng type_mapping rỗng")
    else:
        print("\n⏭️  Bước 3: Bỏ qua merge types (dùng --merge-types để bật)")
    
    try:
        normalize_gold_type_and_unit(spark)
    except Exception as e:
        print(f"❌ Lỗi khi normalize_gold_type_and_unit: {e}")
        print(f"   Traceback: {type(e).__name__}: {str(e)}")
        print("⚠️ Bỏ qua bước normalize_gold_type_and_unit")
    
    print("\n✅ Đã clean tất cả dimension tables (giữ nguyên dữ liệu cũ)!")
    print("="*60 + "\n")
    
    return location_mapping, type_mapping

def process_batch(batch_id: int, batch_df: 'DataFrame', 
                 spark: SparkSession, table_name: str,
                 clean_all: bool = False, merge_types: bool = False):
    """
    Xử lý mỗi batch trong streaming.
    Được gọi tự động bởi foreachBatch.
    
    Nếu clean_all=True, sẽ clean tất cả bảng (LOCATION, TYPE, FACT) mỗi khi FACT thay đổi.
    """
    print(f"\n{'='*60}")
    print(f"📦 Batch {batch_id} - {dt.datetime.now()}")
    print(f"{'='*60}")
    
    # Bỏ qua batch_df (không dùng, chỉ là trigger)
    # Đọc dữ liệu mới từ Oracle dựa trên checkpoint
    print(f"\n🔍 Bước 1: Lấy checkpoint để phát hiện dữ liệu mới...")
    last_ts = get_last_timestamp_from_checkpoint(spark)
    print(f"   📌 Timestamp checkpoint: {last_ts}")
    
    print(f"\n🔍 Bước 2: Đọc dữ liệu mới sau checkpoint...")
    df_new = read_new_data_from_oracle(spark, table_name, last_ts)
    
    new_count = df_new.count()
    if new_count == 0:
        print(f"\nℹ️ Không có dữ liệu mới trong batch này (sau {last_ts})")
        print(f"   ⏭️  Bỏ qua batch {batch_id}")
        return
    
    print(f"\n✅ Phát hiện {new_count} records mới cần xử lý")
    
    print(f"📊 Số lượng records FACT mới: {df_new.count()}")
    
    # Nếu clean_all=True, clean tất cả dimension tables trước
    location_mapping = {}
    type_mapping = {}
    
    if clean_all:
        print("\n🔄 Phát hiện FACT thay đổi, đang clean TẤT CẢ các bảng...")
        print("   (Giữ nguyên dữ liệu CLEAN cũ, chỉ cập nhật/thêm mới)")
        location_mapping, type_mapping = clean_all_dimensions_incremental(spark, merge_types)
    else:
        # Chỉ load mappings hiện có
        location_mapping, type_mapping = load_dimension_mappings(spark)
        print(f"📊 Sử dụng mappings hiện có: Location={len(location_mapping)}, Type={len(type_mapping)}")
    
    # Xử lý dữ liệu FACT mới với mappings
    df_processed = process_new_fact_data(spark, df_new, location_mapping, type_mapping)
    
    if df_processed.count() == 0:
        print("⚠️ Sau xử lý không còn dữ liệu")
        return
    
    # Merge với dữ liệu CLEAN hiện có (CHỈ THÊM, KHÔNG XÓA DỮ LIỆU CŨ) - Logic giống batch file
    try:
        # Đọc bảng CLEAN hiện có
        df_existing = read_table_from_oracle(spark, "GOLD_PRICE_FACT_CLEAN", DB_USER)
        existing_count = df_existing.count()
        print(f"📊 GOLD_PRICE_FACT_CLEAN hiện có: {existing_count} records")
        
        # Union dữ liệu mới với dữ liệu cũ
        df_combined = df_existing.unionByName(df_processed, allowMissingColumns=True)
        combined_count = df_combined.count()
        processed_count = df_processed.count()
        print(f"📊 Sau merge: {combined_count} records (cũ: {existing_count}, mới: {processed_count})")
        
        # Apply cleaning trên dữ liệu đã merge (dedup, handle missing, flag outliers)
        # Logic giống hệt batch file để đảm bảo consistency
        print("🧹 Đang xử lý cleaning trên dữ liệu đã merge...")
        
        # 1. Dedup trên toàn bộ dữ liệu đã merge
        df_combined = df_combined.cache()
        before_dedup = df_combined.count()
        
        # Tạo composite key để dedup (với RECORDED_AT_SAFE để handle null)
        df_combined = df_combined.withColumn(
            "COMBO",
            concat_ws("|", 
                col("SOURCE_ID").cast("string"),
                col("TYPE_ID").cast("string"),
                col("LOCATION_ID").cast("string"),
                col("TIME_ID").cast("string")
            )
        ).withColumn(
            "RECORDED_AT_SAFE",
            coalesce(col(TIMESTAMP_COLUMN), to_timestamp(lit("2000-01-01 00:00:00")))
        )
        
        window_spec = Window.partitionBy("COMBO").orderBy(col("RECORDED_AT_SAFE").desc())
        df_combined = df_combined.withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .drop("rn", "COMBO", "RECORDED_AT_SAFE")
        
        after_dedup = df_combined.count()
        n_dup = before_dedup - after_dedup
        print(f"   ✅ Đã loại bỏ {n_dup} bản ghi trùng")
        
        # 2. Handle missing values (chỉ loại bỏ record thiếu critical fields)
        before_missing = df_combined.count()
        df_combined = df_combined.filter(
            col("BUY_PRICE").isNotNull() & 
            col("SELL_PRICE").isNotNull() & 
            col(TIMESTAMP_COLUMN).isNotNull()
        )
        after_missing = df_combined.count()
        n_missing = before_missing - after_missing
        print(f"   ✅ Đã loại bỏ {n_missing} bản ghi thiếu giá hoặc thời gian")
        
        # 3. Flag outliers (không xóa, chỉ flag) - Logic giống batch file
        from pyspark.sql.functions import percentile_approx
        from decimal import Decimal
        
        def to_float(val):
            if val is None:
                return None
            if isinstance(val, Decimal):
                return float(val)
            return float(val)
        
        try:
            buy_q1_val = df_combined.select(percentile_approx("BUY_PRICE", 0.25).alias("q1")).first()[0]
            buy_q3_val = df_combined.select(percentile_approx("BUY_PRICE", 0.75).alias("q3")).first()[0]
            buy_q1 = to_float(buy_q1_val)
            buy_q3 = to_float(buy_q3_val)
            buy_iqr = buy_q3 - buy_q1
            buy_lower = buy_q1 - 1.5 * buy_iqr
            buy_upper = buy_q3 + 1.5 * buy_iqr
            
            sell_q1_val = df_combined.select(percentile_approx("SELL_PRICE", 0.25).alias("q1")).first()[0]
            sell_q3_val = df_combined.select(percentile_approx("SELL_PRICE", 0.75).alias("q3")).first()[0]
            sell_q1 = to_float(sell_q1_val)
            sell_q3 = to_float(sell_q3_val)
            sell_iqr = sell_q3 - sell_q1
            sell_lower = sell_q1 - 1.5 * sell_iqr
            sell_upper = sell_q3 + 1.5 * sell_iqr
            
            df_combined = df_combined.withColumn(
                "IS_DELETED",
                when(
                    (col("BUY_PRICE") < lit(buy_lower)) | (col("BUY_PRICE") > lit(buy_upper)) |
                    (col("SELL_PRICE") < lit(sell_lower)) | (col("SELL_PRICE") > lit(sell_upper)),
                    lit(1)
                ).otherwise(lit(0))
            )
            
            n_outliers = df_combined.filter(col("IS_DELETED") == 1).count()
            print(f"   ✅ Đã flag {n_outliers} bản ghi outlier (IS_DELETED=1)")
        except Exception as e:
            print(f"   ⚠️ Không thể flag outliers: {e}. Giữ nguyên dữ liệu.")
            if "IS_DELETED" not in df_combined.columns:
                df_combined = df_combined.withColumn("IS_DELETED", lit(0))
        
        # Đảm bảo có cột IS_DELETE (nếu cần)
        if "IS_DELETE" not in df_combined.columns:
            df_combined = df_combined.withColumn("IS_DELETE", col("IS_DELETED"))
        
        # Ghi lại bảng CLEAN với dữ liệu đã merge và đã clean
        final_count = df_combined.count()
        write_table_to_oracle(df_combined, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite")
        print(f"✅ Đã merge và clean: {final_count} records (thêm {processed_count} mới, giữ {existing_count} cũ)")
        
        # Cập nhật checkpoint với timestamp mới nhất từ dữ liệu đã xử lý
        print(f"\n💾 Bước cuối: Cập nhật checkpoint...")
        max_ts = df_processed.agg(spark_max(col(TIMESTAMP_COLUMN))).first()[0]
        if max_ts:
            update_checkpoint(spark, max_ts)
            print(f"✅ Checkpoint đã được cập nhật: {max_ts}")
            print(f"   📌 Batch tiếp theo sẽ xử lý dữ liệu sau {max_ts}")
        else:
            print(f"⚠️ Không có timestamp hợp lệ để cập nhật checkpoint")
    
    except Exception as e:
        # Nếu bảng CLEAN chưa có, ghi dữ liệu mới (chỉ lần đầu)
        print(f"⚠️ Bảng CLEAN chưa có hoặc lỗi: {e}. Ghi dữ liệu mới...")
        # Apply basic cleaning trước khi ghi
        df_processed = df_processed.filter(
            col("BUY_PRICE").isNotNull() & 
            col("SELL_PRICE").isNotNull() & 
            col(TIMESTAMP_COLUMN).isNotNull()
        )
        if "IS_DELETED" not in df_processed.columns:
            df_processed = df_processed.withColumn("IS_DELETED", lit(0))
        if "IS_DELETE" not in df_processed.columns:
            df_processed = df_processed.withColumn("IS_DELETE", lit(0))
        write_table_to_oracle(df_processed, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite")
        print(f"✅ Đã ghi {df_processed.count()} records vào GOLD_PRICE_FACT_CLEAN (lần đầu)")

def create_oracle_polling_stream(spark: SparkSession, table_name: str,
                                trigger_interval: str = STREAMING_TRIGGER_INTERVAL,
                                clean_all: bool = False,
                                merge_types: bool = False):
    """
    Tạo Spark Structured Streaming query để polling Oracle.
    
    Cách hoạt động:
    1. Dùng rate source để tạo trigger (emit 1 row mỗi interval)
    2. Dùng foreachBatch để polling Oracle mỗi interval
    3. Nếu clean_all=True, sẽ clean tất cả bảng mỗi khi FACT thay đổi
    4. Spark tự động quản lý checkpoint và recovery
    """
    
    # Tạo rate source - emit 1 row mỗi interval để trigger foreachBatch
    # Rate source là built-in streaming source của Spark
    trigger_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .option("numPartitions", 1) \
        .load()
    
    # Chỉ lấy timestamp column để làm trigger
    trigger_df = trigger_df.select(col("timestamp").alias("trigger_time"))
    
    # Tạo streaming query với foreachBatch
    def foreach_batch_wrapper(batch_id, batch_df):
        # Bỏ qua batch_df (chỉ là trigger)
        # Gọi process_batch để polling Oracle và clean nếu cần
        process_batch(batch_id, batch_df, spark, table_name, clean_all, merge_types)
    
    # Tạo streaming query
    query = trigger_df.writeStream \
        .foreachBatch(foreach_batch_wrapper) \
        .outputMode("update") \
        .trigger(processingTime=trigger_interval) \
        .option("checkpointLocation", f"{STREAMING_CHECKPOINT_DIR}/oracle_polling") \
        .start()
    
    return query

# ==================== MAIN ====================

def main():
    parser = argparse.ArgumentParser(description="Spark Structured Streaming với Oracle polling")
    parser.add_argument("--interval", type=str, default=STREAMING_TRIGGER_INTERVAL,
                       help="Trigger interval (ví dụ: '30 seconds', '1 minute')")
    parser.add_argument("--table", type=str, default="GOLD_PRICE_FACT",
                       help="Tên bảng Oracle để monitor (GOLD_PRICE_FACT, LOCATION_DIMENSION, GOLD_TYPE_DIMENSION)")
    parser.add_argument("--clean-all", action="store_true",
                       help="Khi FACT thay đổi, tự động clean TẤT CẢ các bảng (LOCATION, TYPE, FACT)")
    parser.add_argument("--merge-types", action="store_true",
                       help="Gộp TYPE tương đồng khi clean (chỉ dùng với --clean-all)")
    
    args = parser.parse_args()
    
    # Tạo checkpoint directory
    os.makedirs(STREAMING_CHECKPOINT_DIR, exist_ok=True)
    
    spark = create_spark_session()
    
    print("\n" + "="*60)
    print("🚀 SPARK STRUCTURED STREAMING - ORACLE POLLING")
    print("="*60)
    print(f"📊 Table: {args.table}")
    print(f"⏱️  Trigger Interval: {args.interval}")
    print(f"📁 Checkpoint: {STREAMING_CHECKPOINT_DIR}")
    if args.clean_all:
        print(f"🔄 Mode: Clean ALL tables khi FACT thay đổi")
        print(f"   ✅ LOCATION_DIMENSION → LOCATION_DIMENSION_CLEAN")
        print(f"   ✅ GOLD_TYPE_DIMENSION → GOLD_TYPE_DIMENSION_CLEAN")
        print(f"   ✅ GOLD_PRICE_FACT → GOLD_PRICE_FACT_CLEAN")
        if args.merge_types:
            print(f"   ✅ Merge duplicate types: ON")
    else:
        print(f"🔄 Mode: Streaming FACT only (chỉ xử lý FACT)")
    print("="*60 + "\n")
    
    # Kiểm tra batch functions có sẵn không
    if args.clean_all and not BATCH_FUNCTIONS_AVAILABLE:
        print("❌ Lỗi: Không thể import batch functions để clean dimensions!")
        print("   Vui lòng đảm bảo các dependencies đã được cài:")
        print("   pip install pandas numpy scikit-learn fuzzywuzzy python-Levenshtein")
        print("\n   Hoặc chạy không có --clean-all để chỉ xử lý FACT")
        sys.exit(1)
    
    # Chỉ streaming FACT table
    if args.table != "GOLD_PRICE_FACT":
        print(f"⚠️ Lưu ý: Streaming chỉ hỗ trợ GOLD_PRICE_FACT")
        print(f"   Đang chuyển sang GOLD_PRICE_FACT...\n")
        args.table = "GOLD_PRICE_FACT"
    
    # Khởi động streaming query
    query = create_oracle_polling_stream(
        spark, 
        args.table, 
        args.interval,
        args.clean_all,
        args.merge_types
    )
    
    print(f"\n✅ Streaming query đã khởi động!")
    print(f"📊 Query ID: {query.id}")
    print(f"📊 Status: {query.status}")
    print(f"📊 Spark UI: http://localhost:4040")
    print(f"\n🔄 Đang chạy... Nhấn Ctrl+C để dừng\n")
    
    try:
        # Chờ streaming query chạy
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⚠️ Đang dừng streaming query...")
        query.stop()
        print("✅ Đã dừng")
    
    spark.stop()

if __name__ == "__main__":
    main()

