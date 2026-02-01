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
from typing import Dict, List, Tuple, Optional

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

# ====================== CONFIG ======================
DB_USER = "CLOUD"
DB_PASS = "cloud123"
DB_HOST = "136.110.60.196"
DB_PORT = "1521"
DB_SERVICE = "XEPDB1"

DB_DSN = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE}"
DB_URL = f"jdbc:oracle:thin:@{DB_DSN}"

SNAPSHOT_DIR = "./snapshots"
JOB_NAME = "DAILY_GOLD_JOB_STREAMING_ORACLE"
SIM_THRESHOLD_LOC = 0.80
SIM_THRESHOLD_TYPE = 0.75
FUZZY_FALLBACK = 90

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
    """
    schema_prefix = f'"{DB_USER}"."'
    full_table = f'{schema_prefix}{table_name}"'
    
    # Tạo query để chỉ lấy dữ liệu mới
    ts_str = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    query = f"""
        (SELECT * FROM {full_table}
         WHERE {timestamp_column} > TO_TIMESTAMP('{ts_str}', 'YYYY-MM-DD HH24:MI:SS')
         ORDER BY {timestamp_column})
    """
    
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
            .option("dbtable", query) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
        return df
    except Exception as e:
        print(f"⚠️ Lỗi khi đọc dữ liệu mới: {e}")
        return spark.createDataFrame([], get_fact_schema())

def get_last_timestamp_from_checkpoint(spark: SparkSession) -> dt.datetime:
    """Lấy timestamp cuối cùng từ checkpoint."""
    try:
        df = read_table_from_oracle(spark, "ETL_CHECKPOINT", DB_USER)
        df_checkpoint = df.filter(col("JOB_NAME") == JOB_NAME)
        
        if df_checkpoint.count() > 0:
            last_run = df_checkpoint.select("LAST_RUN").first()
            if last_run and last_run[0]:
                return last_run[0]
    except Exception as e:
        print(f"⚠️ Không đọc được checkpoint: {e}")
    
    # Nếu chưa có checkpoint, lấy timestamp từ bảng FACT
    try:
        df_fact = read_table_from_oracle(spark, "GOLD_PRICE_FACT", DB_USER)
        if df_fact.count() > 0:
            max_ts = df_fact.agg(spark_max(col(TIMESTAMP_COLUMN))).first()[0]
            if max_ts:
                return max_ts
    except Exception as e:
        print(f"⚠️ Không lấy được timestamp từ FACT: {e}")
    
    return dt.datetime(2000, 1, 1)

def update_checkpoint(spark: SparkSession, ts: dt.datetime):
    """Cập nhật checkpoint."""
    checkpoint_df = spark.createDataFrame(
        [(JOB_NAME, ts)],
        ["JOB_NAME", "LAST_RUN"]
    )
    
    try:
        existing = read_table_from_oracle(spark, "ETL_CHECKPOINT", DB_USER)
        combined = existing.filter(col("JOB_NAME") != JOB_NAME).union(checkpoint_df)
    except:
        combined = checkpoint_df
    
    combined.write \
        .format("jdbc") \
        .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
        .option("dbtable", f"{DB_USER}.ETL_CHECKPOINT") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .mode("overwrite") \
        .save()

def write_table_to_oracle(df: 'DataFrame', table_name: str, mode: str = "append"):
    """Ghi DataFrame vào Oracle DB."""
    if df.count() == 0:
        return
    
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
        .option("dbtable", table_name) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .mode(mode) \
        .save()

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

def process_batch(batch_id: int, batch_df: 'DataFrame', 
                 spark: SparkSession, table_name: str,
                 location_mapping: Dict, type_mapping: Dict):
    """
    Xử lý mỗi batch trong streaming.
    Được gọi tự động bởi foreachBatch.
    """
    print(f"\n{'='*60}")
    print(f"📦 Batch {batch_id} - {dt.datetime.now()}")
    print(f"{'='*60}")
    
    # Bỏ qua batch_df (không dùng, chỉ là trigger)
    # Đọc dữ liệu mới từ Oracle
    last_ts = get_last_timestamp_from_checkpoint(spark)
    print(f"🔍 Đang kiểm tra dữ liệu mới sau {last_ts}...")
    
    df_new = read_new_data_from_oracle(spark, table_name, last_ts)
    
    if df_new.count() == 0:
        print("ℹ️ Không có dữ liệu mới trong batch này")
        return
    
    print(f"📊 Số lượng records mới: {df_new.count()}")
    
    # Xử lý dữ liệu mới
    df_processed = process_new_fact_data(spark, df_new, location_mapping, type_mapping)
    
    if df_processed.count() == 0:
        print("⚠️ Sau xử lý không còn dữ liệu")
        return
    
    # Merge với dữ liệu CLEAN hiện có (để dedup toàn bộ)
    try:
        df_existing = read_table_from_oracle(spark, "GOLD_PRICE_FACT_CLEAN", DB_USER)
        df_combined = df_existing.unionByName(df_processed, allowMissingColumns=True)
        
        # Deduplicate toàn bộ
        df_combined = df_combined.withColumn(
            "COMBO",
            concat_ws("|",
                col("SOURCE_ID").cast("string"),
                col("TYPE_ID").cast("string"),
                col("LOCATION_ID").cast("string"),
                col("TIME_ID").cast("string")
            )
        )
        
        window_spec = Window.partitionBy("COMBO").orderBy(col(TIMESTAMP_COLUMN).desc())
        df_final = df_combined.withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .drop("rn", "COMBO")
        
        # Ghi lại bảng CLEAN
        write_table_to_oracle(df_final, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite")
        print(f"✅ Đã cập nhật GOLD_PRICE_FACT_CLEAN: {df_final.count()} records")
        
        # Cập nhật checkpoint với timestamp mới nhất
        max_ts = df_processed.agg(spark_max(col(TIMESTAMP_COLUMN))).first()[0]
        if max_ts:
            update_checkpoint(spark, max_ts)
            print(f"✅ Đã cập nhật checkpoint: {max_ts}")
    
    except Exception as e:
        print(f"⚠️ Lỗi khi merge với CLEAN: {e}")
        # Nếu lỗi, chỉ append dữ liệu mới
        write_table_to_oracle(df_processed, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "append")
        print(f"✅ Đã append {df_processed.count()} records mới")

def create_oracle_polling_stream(spark: SparkSession, table_name: str,
                                location_mapping: Dict, type_mapping: Dict,
                                trigger_interval: str = STREAMING_TRIGGER_INTERVAL):
    """
    Tạo Spark Structured Streaming query để polling Oracle.
    
    Cách hoạt động:
    1. Dùng rate source để tạo trigger (emit 1 row mỗi interval)
    2. Dùng foreachBatch để polling Oracle mỗi interval
    3. Spark tự động quản lý checkpoint và recovery
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
        # Gọi process_batch để polling Oracle
        process_batch(batch_id, batch_df, spark, table_name, location_mapping, type_mapping)
    
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
                       help="Tên bảng Oracle để monitor")
    
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
    print("="*60 + "\n")
    
    # Load dimension mappings (chạy một lần, có thể refresh định kỳ)
    print("📊 Đang load dimension mappings...")
    location_mapping, type_mapping = load_dimension_mappings(spark)
    print(f"✅ Location mappings: {len(location_mapping)}")
    print(f"✅ Type mappings: {len(type_mapping)}")
    
    # Khởi động streaming query
    query = create_oracle_polling_stream(
        spark, 
        args.table, 
        location_mapping, 
        type_mapping,
        args.interval
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

