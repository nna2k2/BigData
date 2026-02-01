# -*- coding: utf-8 -*-
"""
Daily Gold ETL Job (Spark version - tạo bảng _CLEAN)
- B1: LOCATION_DIMENSION: phát hiện thành phố trùng ngữ nghĩa -> gộp về 1 ID và tạo LOCATION_DIMENSION_CLEAN
- B2: GOLD_TYPE_DIMENSION: dùng tương đồng để điền PURITY/CATEGORY còn thiếu -> tạo GOLD_TYPE_DIMENSION_CLEAN
- B3: GOLD_PRICE_FACT: với (SOURCE_ID, TYPE_ID, LOCATION_ID, TIME_ID) trùng nhau -> giữ RECORDED_AT mới nhất, tạo GOLD_PRICE_FACT_CLEAN
- Incremental bằng checkpoint trong DB (bảng ETL_CHECKPOINT)
- Chụp snapshot trước/sau ra CSV để báo cáo
"""

import argparse
import datetime as dt
import os
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, trim, upper, lower, regexp_replace, 
    concat_ws, first, last, max as spark_max, min as spark_min,
    count, isnan, isnull, coalesce, to_timestamp, date_format,
    row_number, window, monotonically_increasing_id
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.window import Window

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from fuzzywuzzy import fuzz

import re
import unicodedata

# ====================== CONFIG ======================
DB_USER = "CLOUD"
DB_PASS = "cloud123"
DB_HOST = "136.110.60.196"
DB_PORT = "1521"
DB_SERVICE = "XEPDB1"

# Tạo DSN và JDBC URL từ các thông số
DB_DSN = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE}"
DB_URL = f"jdbc:oracle:thin:@{DB_DSN}"

SNAPSHOT_DIR = "./snapshots"
JOB_NAME = "DAILY_GOLD_JOB"
SIM_THRESHOLD_LOC = 0.80
SIM_THRESHOLD_TYPE = 0.75
FUZZY_FALLBACK = 90

# Spark config
SPARK_APP_NAME = "DailyGoldETLJob"
SPARK_MASTER = "local[*]"  # hoặc "yarn" nếu chạy trên cluster

# JDBC Driver path (có thể chỉ định đường dẫn tùy chỉnh)
# Để None để tự động tìm, hoặc chỉ định đường dẫn đầy đủ
OJDBC_JAR_PATH = None  # Ví dụ: r"E:\THAC SI\BIGDATA\libs\ojdbc8.jar"

# ====================================================

def create_spark_session(ojdbc_path: str = None, java_home: str = None):
    """Tạo SparkSession với Oracle JDBC driver."""
    # Set JAVA_HOME nếu được chỉ định
    if java_home and os.path.exists(java_home):
        os.environ['JAVA_HOME'] = java_home
        print(f"✅ Đã set JAVA_HOME: {java_home}")
    elif 'JAVA_HOME' not in os.environ:
        # Tự động tìm Java 17/11 trong các vị trí phổ biến
        possible_java_homes = [
            r"C:\Program Files\Java\jdk-17",
            r"C:\Program Files\Java\jdk-11",
            r"C:\Program Files\Eclipse Adoptium\jdk-17.0.0-hotspot",
            r"C:\Program Files\Eclipse Adoptium\jdk-11.0.0-hotspot",
            os.path.join(os.path.dirname(__file__), "jdk-17"),
            os.path.join(os.path.dirname(__file__), "jdk-11"),
        ]
        for java_path in possible_java_homes:
            java_exe = os.path.join(java_path, "bin", "java.exe")
            if os.path.exists(java_exe):
                os.environ['JAVA_HOME'] = java_path
                print(f"✅ Tự động tìm thấy Java: {java_path}")
                break
    
    builder = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g")
    
    # Xác định đường dẫn JDBC driver
    # Ưu tiên: 1) tham số hàm, 2) config OJDBC_JAR_PATH, 3) tự động tìm
    final_path = None
    
    if ojdbc_path:
        final_path = ojdbc_path
    elif OJDBC_JAR_PATH:
        final_path = OJDBC_JAR_PATH
    else:
        # Tự động tìm ojdbc8.jar trong các thư mục phổ biến
        possible_paths = [
            "ojdbc8.jar",
            "./ojdbc8.jar",
            "../ojdbc8.jar",
            os.path.join(os.path.dirname(__file__), "ojdbc8.jar"),
            os.path.join(os.path.dirname(__file__), "libs", "ojdbc8.jar"),
            os.path.join(os.path.dirname(__file__), "jars", "ojdbc8.jar"),
        ]
        for path in possible_paths:
            if os.path.exists(path):
                final_path = os.path.abspath(path)
                break
    
    # Thêm JDBC driver vào Spark config
    if final_path:
        if os.path.exists(final_path):
            builder = builder.config("spark.jars", final_path)
            print(f"✅ Đã load JDBC driver từ: {final_path}")
        else:
            print(f"⚠️ Không tìm thấy JDBC driver tại: {final_path}")
            print(f"   Vui lòng tải ojdbc8.jar và đặt vào thư mục dự án")
            print(f"   Xem hướng dẫn: HUONG_DAN_TAI_OJDBC.md")
    else:
        print("⚠️ Không tìm thấy ojdbc8.jar")
        print("   Vui lòng:")
        print("   1. Tải ojdbc8.jar từ Oracle hoặc Maven")
        print("   2. Đặt file vào thư mục dự án")
        print("   3. Hoặc chỉ định đường dẫn trong OJDBC_JAR_PATH")
        print("   Xem hướng dẫn: HUONG_DAN_TAI_OJDBC.md")
    
    spark = builder.getOrCreate()
    return spark

def read_table_from_oracle(spark: SparkSession, table_name: str, schema: str = None) -> 'DataFrame':
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

def write_table_to_oracle(df: 'DataFrame', table_name: str, mode: str = "overwrite"):
    """Ghi DataFrame vào Oracle DB."""
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
        .option("dbtable", table_name) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .mode(mode) \
        .save()

def ensure_checkpoint_table(spark: SparkSession):
    """Đảm bảo bảng ETL_CHECKPOINT tồn tại, nếu chưa có thì tạo."""
    try:
        # Thử đọc bảng để kiểm tra xem có tồn tại không
        read_table_from_oracle(spark, "ETL_CHECKPOINT", DB_USER)
        print("✅ Bảng ETL_CHECKPOINT đã tồn tại")
    except Exception as e:
        # Bảng chưa tồn tại, tạo mới
        print("⚠️ Bảng ETL_CHECKPOINT chưa tồn tại, đang tạo mới...")
        try:
            # Tạo bảng bằng cách tạo DataFrame rỗng với schema đúng và ghi vào
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType
            
            schema = StructType([
                StructField("JOB_NAME", StringType(), False),
                StructField("LAST_RUN", TimestampType(), True)
            ])
            
            empty_df = spark.createDataFrame([], schema)
            write_table_to_oracle(empty_df, f"{DB_USER}.ETL_CHECKPOINT", "overwrite")
            print("✅ Đã tạo bảng ETL_CHECKPOINT")
        except Exception as create_error:
            print(f"⚠️ Không thể tạo bảng ETL_CHECKPOINT tự động: {create_error}")
            print("   Vui lòng tạo bảng thủ công bằng SQL:")
            print(f"   CREATE TABLE {DB_USER}.ETL_CHECKPOINT (")
            print(f"       JOB_NAME VARCHAR2(100) PRIMARY KEY,")
            print(f"       LAST_RUN TIMESTAMP")
            print(f"   );")
            print("   Hoặc chạy file: create_etl_checkpoint.sql")

def get_last_checkpoint(spark: SparkSession) -> dt.datetime:
    """Lấy checkpoint cuối cùng từ ETL_CHECKPOINT."""
    # Đảm bảo bảng tồn tại
    ensure_checkpoint_table(spark)
    
    try:
        df = read_table_from_oracle(spark, "ETL_CHECKPOINT", DB_USER)
        df_checkpoint = df.filter(col("JOB_NAME") == JOB_NAME)
        
        if df_checkpoint.count() > 0:
            last_run = df_checkpoint.select("LAST_RUN").first()
            if last_run and last_run[0]:
                return last_run[0]
    except Exception as e:
        print(f"⚠️ Không đọc được checkpoint: {e}")
    
    return dt.datetime(2000, 1, 1)

def set_checkpoint(spark: SparkSession, ts: dt.datetime):
    """Cập nhật checkpoint."""
    # Đảm bảo bảng tồn tại
    ensure_checkpoint_table(spark)
    
    checkpoint_df = spark.createDataFrame(
        [(JOB_NAME, ts)],
        ["JOB_NAME", "LAST_RUN"]
    )
    
    # Merge checkpoint (read existing, union, dedup, write)
    try:
        existing = read_table_from_oracle(spark, "ETL_CHECKPOINT", DB_USER)
        combined = existing.filter(col("JOB_NAME") != JOB_NAME).union(checkpoint_df)
    except:
        combined = checkpoint_df
    
    write_table_to_oracle(combined, f"{DB_USER}.ETL_CHECKPOINT", "overwrite")

def snapshot_table(df: 'DataFrame', table: str, tag: str):
    """Chụp snapshot DataFrame ra CSV."""
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    path = os.path.join(SNAPSHOT_DIR, f"{table}_{tag}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    
    # Convert Spark DataFrame to Pandas for CSV export
    pandas_df = df.toPandas()
    pandas_df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"📸 Snapshot {table} -> {path}")

# -------------------- LOCATION normalize --------------------

def norm_txt(s: str) -> str:
    """Chuẩn hoá tiếng Việt (bỏ dấu, lowercase, trim) để so khớp ổn định."""
    if not s:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s

POSITIVE_SYNONYMS = {
    ("ho chi minh", "tphcm"),
    ("ho chi minh", "tp hcm"),
    ("da nang", "danang"),
    ("ha noi", "thu do"),
    ("ha noi", "hn"),
}

NEGATIVE_SYNONYMS = {
    ("sai gon", "ho chi minh"),
    ("sai gon", "tphcm"),
    ("sai gon", "tp hcm"),
}

def pair_blocked(a: str, b: str) -> bool:
    A, B = norm_txt(a), norm_txt(b)
    return (A, B) in NEGATIVE_SYNONYMS or (B, A) in NEGATIVE_SYNONYMS

def pair_forced(a: str, b: str) -> bool:
    A, B = norm_txt(a), norm_txt(b)
    return (A, B) in POSITIVE_SYNONYMS or (B, A) in POSITIVE_SYNONYMS

def build_similarity_groups(values: List[str], threshold: float) -> List[List[int]]:
    """Nhóm các index có độ tương đồng cosine TF-IDF >= threshold + fallback fuzzy."""
    if not values:
        return []

    vec = TfidfVectorizer(ngram_range=(1, 2), analyzer='char_wb').fit(values)
    tf = vec.transform(values)
    sim = cosine_similarity(tf)

    n = len(values)
    visited = [False] * n
    groups = []

    for i in range(n):
        if visited[i]:
            continue
        group = [i]
        visited[i] = True

        for j in range(i + 1, n):
            if visited[j]:
                continue

            a, b = values[i], values[j]

            if pair_blocked(a, b):
                print(f"🚫 BLOCKED: '{a}' vs '{b}'")
                continue

            if pair_forced(a, b):
                print(f"✅ FORCED GROUP: '{a}' ~ '{b}'")
                group.append(j)
                visited[j] = True
                continue

            if sim[i, j] >= threshold or fuzz.token_set_ratio(a, b) >= FUZZY_FALLBACK:
                print(f"≈ SIMILAR: '{a}' ~ '{b}' (cos={sim[i,j]:.2f}, fuzzy={fuzz.token_set_ratio(a,b)})")
                group.append(j)
                visited[j] = True

        groups.append(group)
    return groups

def normalize_locations(spark: SparkSession) -> Dict[int, int]:
    """Phát hiện & gộp các LOCATION tương đồng; tạo LOCATION_DIMENSION_CLEAN."""
    df_loc = read_table_from_oracle(spark, "LOCATION_DIMENSION", DB_USER)
    
    if df_loc.count() == 0:
        print("⚠️ LOCATION_DIMENSION trống.")
        return {}

    snapshot_table(df_loc, "LOCATION_DIMENSION", "before_loc_norm")

    # Convert to Pandas for similarity computation
    pandas_loc = df_loc.toPandas()
    names = pandas_loc["CITY"].astype(str).fillna("").str.lower().tolist()
    groups = build_similarity_groups(names, SIM_THRESHOLD_LOC)

    mapping = {}
    for grp in groups:
        ids = pandas_loc.iloc[grp]["ID"].tolist()
        canon = min(ids)
        for idx in grp:
            lid = int(pandas_loc.iloc[idx]["ID"])
            if lid != canon:
                mapping[lid] = canon

    print(f"🔎 Mapping location (old->new): {mapping}")

    # Áp dụng mapping để tạo bảng CLEAN
    # Đảm bảo TẤT CẢ dữ liệu gốc đều có trong CLEAN (chỉ merge ID, không mất record)
    if mapping:
        # Tạo mapping DataFrame
        mapping_df = spark.createDataFrame(
            [(k, v) for k, v in mapping.items()],
            ["OLD_ID", "NEW_ID"]
        )
        
        # Join LEFT để đảm bảo tất cả record gốc đều được giữ lại
        df_clean = df_loc.join(
            mapping_df,
            df_loc["ID"] == mapping_df["OLD_ID"],
            "left"
        ).withColumn(
            "ID_CLEAN",
            when(col("NEW_ID").isNotNull(), col("NEW_ID"))
            .otherwise(col("ID"))  # Giữ nguyên ID nếu không có mapping
        ).select(
            col("ID_CLEAN").alias("ID"),
            col("CITY"),
            col("REGION")
        )
    else:
        # Không có mapping, copy toàn bộ dữ liệu gốc
        df_clean = df_loc.select("ID", "CITY", "REGION")

    # Lấy distinct để loại bỏ duplicate sau khi merge (chỉ loại bỏ những record trùng hoàn toàn)
    df_final = df_clean.distinct()
    
    # Log số lượng record
    original_count = df_loc.count()
    final_count = df_final.count()
    print(f"📊 LOCATION_DIMENSION: {original_count} records -> LOCATION_DIMENSION_CLEAN: {final_count} records")
    
    # Đảm bảo luôn có dữ liệu trong bảng CLEAN (copy toàn bộ nếu cần)
    if final_count == 0 and original_count > 0:
        print("⚠️ Cảnh báo: Bảng CLEAN rỗng nhưng bảng gốc có dữ liệu! Copy toàn bộ dữ liệu gốc...")
        df_final = df_loc.select("ID", "CITY", "REGION")
        final_count = original_count

    # Ghi vào bảng _CLEAN (luôn có dữ liệu, kể cả không có gì để clean)
    write_table_to_oracle(df_final, f"{DB_USER}.LOCATION_DIMENSION_CLEAN", "overwrite")
    snapshot_table(df_final, "LOCATION_DIMENSION_CLEAN", "after_loc_norm")
    
    return mapping

# -------------------- GOLD TYPE enrichment --------------------

def enrich_gold_types(spark: SparkSession) -> Tuple[int, int]:
    """Làm giàu GOLD_TYPE_DIMENSION và tạo GOLD_TYPE_DIMENSION_CLEAN."""
    df = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION", DB_USER)
    
    if df.count() == 0:
        print("⚠️ GOLD_TYPE_DIMENSION trống.")
        return (0, 0)

    snapshot_table(df, "GOLD_TYPE_DIMENSION", "before_type_enrich")

    pandas_df = df.toPandas()
    values = pandas_df["TYPE_NAME"].astype(str).str.lower().fillna("").tolist()
    groups = build_similarity_groups(values, SIM_THRESHOLD_TYPE)

    purity_fill = 0
    category_fill = 0
    
    # Tạo mapping dictionary để update
    updates = {}
    
    for grp in groups:
        sub = pandas_df.iloc[grp]
        
        known_purity = sub["PURITY"].dropna()
        known_purity = known_purity[~known_purity.astype(str).str.lower().isin(["unknown", "nan", "none", ""])]
        known_cat = sub["CATEGORY"].dropna()
        known_cat = known_cat[~known_cat.astype(str).str.lower().isin(["unknown", "nan", "none", "other", ""])]
        
        purity_mode = known_purity.mode().iloc[0] if not known_purity.empty else "99.99%"
        cat_mode = known_cat.mode().iloc[0] if not known_cat.empty else "Gold bar"
        
        for _, row in sub.iterrows():
            tid = int(row["ID"])
            purity = str(row["PURITY"]).strip().lower() if pd.notna(row["PURITY"]) else ""
            cat = str(row["CATEGORY"]).strip().lower() if pd.notna(row["CATEGORY"]) else ""
            
            if purity in ["", "unknown", "nan", "none"]:
                updates[tid] = updates.get(tid, {})
                updates[tid]["PURITY"] = purity_mode
                purity_fill += 1
            
            if cat in ["", "unknown", "nan", "none", "other"]:
                updates[tid] = updates.get(tid, {})
                updates[tid]["CATEGORY"] = cat_mode
                category_fill += 1

    # Apply updates to Spark DataFrame
    # Đảm bảo luôn copy toàn bộ dữ liệu gốc, kể cả không có gì để enrich
    df_enriched = df
    if updates:  # Chỉ update nếu có thay đổi
        for tid, update_dict in updates.items():
            if "PURITY" in update_dict:
                df_enriched = df_enriched.withColumn(
                    "PURITY",
                    when(col("ID") == tid, lit(update_dict["PURITY"]))
                    .otherwise(col("PURITY"))
                )
            if "CATEGORY" in update_dict:
                df_enriched = df_enriched.withColumn(
                    "CATEGORY",
                    when(col("ID") == tid, lit(update_dict["CATEGORY"]))
                    .otherwise(col("CATEGORY"))
                )
    # Nếu không có updates, df_enriched = df (giữ nguyên toàn bộ dữ liệu gốc)

    # Log số lượng record
    original_count = df.count()
    enriched_count = df_enriched.count()
    print(f"📊 GOLD_TYPE_DIMENSION: {original_count} records -> GOLD_TYPE_DIMENSION_CLEAN: {enriched_count} records")
    
    # Đảm bảo luôn có dữ liệu trong bảng CLEAN (copy toàn bộ nếu cần)
    if enriched_count == 0 and original_count > 0:
        print("⚠️ Cảnh báo: Bảng CLEAN rỗng nhưng bảng gốc có dữ liệu! Copy toàn bộ dữ liệu gốc...")
        df_enriched = df
        enriched_count = original_count
    
    # Ghi vào bảng _CLEAN (luôn có dữ liệu, kể cả không có gì để clean)
    write_table_to_oracle(df_enriched, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
    snapshot_table(df_enriched, "GOLD_TYPE_DIMENSION_CLEAN", "after_type_enrich")

    print(f"✨ Đã fill PURITY: {purity_fill}, CATEGORY: {category_fill}")
    return (purity_fill, category_fill)

def normalize_purity_format(spark: SparkSession) -> int:
    """Chuẩn hoá cột PURITY trong GOLD_TYPE_DIMENSION_CLEAN về dạng 'xx.xx%'."""
    df = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION_CLEAN", DB_USER)
    
    if df.count() == 0:
        print("⚠️ GOLD_TYPE_DIMENSION_CLEAN trống.")
        return 0

    def normalize_purity_udf(purity):
        if not purity:
            return None
        s = str(purity).strip()
        s = s.replace("%", "").replace(" ", "").replace(",", ".").lower()
        if s in ["", "none", "nan", "unknown", "unk"]:
            return None
        nums = re.findall(r"[\d\.]+", s)
        if not nums:
            return None
        try:
            val = float(nums[0])
            if val <= 0 or val > 100:
                return None
            return f"{val:.2f}%"
        except:
            return None

    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    
    normalize_purity = udf(normalize_purity_udf, StringType())
    
    df_clean = df.withColumn(
        "PURITY",
        normalize_purity(col("PURITY"))
    )

    # Count changes by comparing before/after
    df_joined = df.alias("before").join(
        df_clean.alias("after"),
        df["ID"] == df_clean["ID"],
        "inner"
    )
    changed_count = df_joined.filter(
        (col("before.PURITY") != col("after.PURITY")) |
        (col("before.PURITY").isNull() & col("after.PURITY").isNotNull()) |
        (col("before.PURITY").isNotNull() & col("after.PURITY").isNull())
    ).count()
    
    write_table_to_oracle(df_clean, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
    
    print(f"🔧 Đã chuẩn hoá PURITY cho {changed_count} bản ghi.")
    return changed_count

def normalize_text(s: str) -> str:
    """Chuẩn hoá text dạng lowercase, bỏ ký tự đặc biệt."""
    if not s:
        return ""
    s = re.sub(r'[^A-Za-z0-9]+', ' ', str(s))
    s = re.sub(r'\s+', ' ', s).strip().lower()
    tokens = sorted(s.split())
    return " ".join(tokens)

def normalize_category_smart(spark: SparkSession) -> int:
    """Chuẩn hoá CATEGORY trong GOLD_TYPE_DIMENSION_CLEAN."""
    df = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION_CLEAN", DB_USER)
    
    if df.count() == 0:
        print("⚠️ GOLD_TYPE_DIMENSION_CLEAN trống.")
        return 0

    pandas_df = df.toPandas()
    pandas_df["CLEAN"] = pandas_df["CATEGORY"].astype(str).apply(normalize_text)
    unique_vals = pandas_df["CLEAN"].unique().tolist()

    groups = []
    visited = set()
    for i, base in enumerate(unique_vals):
        if base in visited:
            continue
        group = [base]
        visited.add(base)
        for j, other in enumerate(unique_vals):
            if other in visited:
                continue
            if fuzz.token_set_ratio(base, other) >= 90:
                group.append(other)
                visited.add(other)
        groups.append(group)

    mapping = {}
    for grp in groups:
        canon = sorted(grp, key=len)[0]
        for val in grp:
            mapping[val] = canon

    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    
    def title_case_udf(s):
        return s.title() if s else None
    
    title_case = udf(title_case_udf, StringType())
    
    def map_category_udf(cat):
        clean = normalize_text(cat)
        canon = mapping.get(clean, clean)
        return canon.title() if canon else None
    
    map_category = udf(map_category_udf, StringType())
    
    df_clean = df.withColumn("CATEGORY", map_category(col("CATEGORY")))
    
    changed_count = df.join(df_clean, df["ID"] == df_clean["ID"], "inner") \
        .filter(df["CATEGORY"] != df_clean["CATEGORY"]).count()
    
    write_table_to_oracle(df_clean, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
    
    print(f"✅ Đã chuẩn hoá CATEGORY cho {changed_count} bản ghi.")
    return changed_count

def merge_duplicate_types_and_update_fact(spark: SparkSession):
    """Gộp các bản ghi GOLD_TYPE_DIMENSION trùng và tạo mapping."""
    df = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION_CLEAN", DB_USER)
    
    if df.count() == 0:
        print("⚠️ GOLD_TYPE_DIMENSION_CLEAN trống.")
        return {}

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

    # Group by normalized values and find canonical ID
    window_spec = Window.partitionBy(*partition_cols).orderBy("ID")
    
    df_with_canon = df_normalized.withColumn(
        "CANON_ID",
        first("ID").over(window_spec)
    )

    # Create mapping
    mapping_df = df_with_canon.filter(col("ID") != col("CANON_ID")) \
        .select(col("ID").alias("OLD_ID"), col("CANON_ID").alias("NEW_ID")) \
        .distinct()
    
    mapping = {}
    if mapping_df.count() > 0:
        for row in mapping_df.collect():
            mapping[int(row["OLD_ID"])] = int(row["NEW_ID"])
    
    # Create clean table - Đảm bảo TẤT CẢ dữ liệu gốc đều có trong CLEAN
    # Chỉ select các cột có tồn tại
    select_cols = ["TYPE_NAME", "PURITY", "CATEGORY"]
    if "BRAND" in columns:
        select_cols.append("BRAND")
    
    # Lấy tất cả record với ID đã được normalize (canonical ID)
    # Điều này đảm bảo tất cả record gốc đều có trong CLEAN (chỉ ID được merge)
    df_clean = df_with_canon.select(
        col("CANON_ID").alias("ID"),
        *[col(c) for c in select_cols]
    ).distinct()
    
    # Đảm bảo số lượng không bị mất quá nhiều
    original_count = df.count()
    clean_count = df_clean.count()
    if clean_count < original_count * 0.8:  # Nếu mất > 20% thì có vấn đề
        print(f"⚠️ Cảnh báo: Số lượng record giảm từ {original_count} xuống {clean_count}")
    
    write_table_to_oracle(df_clean, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
    
    print(f"✅ Đã gộp {len(mapping)} TYPE trùng. Giữ lại {clean_count}/{original_count} records trong CLEAN.")
    return mapping

def normalize_gold_type_and_unit(spark: SparkSession):
    """Chuẩn hóa BRAND và UNIT."""
    df_type = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION_CLEAN", DB_USER)
    
    # Kiểm tra xem cột BRAND có tồn tại không
    columns = df_type.columns
    df_type_clean = df_type
    
    if "BRAND" in columns:
        df_type_clean = df_type_clean.withColumn(
            "BRAND",
            when(col("BRAND").isNotNull(),
                 upper(trim(regexp_replace(regexp_replace(col("BRAND"), "\\.", ""), "VÀNG ", ""))))
            .otherwise(col("BRAND"))
        )
        print("📏 Đã chuẩn hóa BRAND.")
    else:
        print("⚠️ Cột BRAND không tồn tại trong GOLD_TYPE_DIMENSION_CLEAN, bỏ qua chuẩn hóa BRAND.")
    
    write_table_to_oracle(df_type_clean, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")

# -------------------- FACT dedup incremental --------------------

def dedup_fact_incremental(spark: SparkSession, last_run: dt.datetime, location_mapping: Dict, type_mapping: Dict):
    """Deduplicate FACT và cập nhật GOLD_PRICE_FACT_CLEAN."""
    # Đọc từ bảng CLEAN đã được tạo (mappings đã được apply)
    df_fact = read_table_from_oracle(spark, "GOLD_PRICE_FACT_CLEAN", DB_USER)
    
    if df_fact.count() == 0:
        print("ℹ️ Không có FACT để dedup.")
        return 0

    before_count = df_fact.count()

    # Create combo key
    df_fact = df_fact.withColumn(
        "COMBO",
        concat_ws("_", 
                  col("SOURCE_ID").cast("string"),
                  col("TYPE_ID").cast("string"),
                  col("LOCATION_ID").cast("string"),
                  col("TIME_ID").cast("string"))
    )

    # Keep latest record per combo
    window_spec = Window.partitionBy("COMBO").orderBy(col("RECORDED_AT").desc())
    df_clean = df_fact.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn", "COMBO") \
        .withColumn("IS_DELETED", lit(0)) \
        .withColumn("IS_DELETE", lit(0))

    n_dup = before_count - df_clean.count()

    write_table_to_oracle(df_clean, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite")
    snapshot_table(df_clean, "GOLD_PRICE_FACT_CLEAN", "after_fact_dedup")
    
    print(f"🧹 Đã tạo GOLD_PRICE_FACT_CLEAN với {df_clean.count()} bản ghi (loại bỏ {n_dup} trùng).")
    return n_dup

def handle_missing_values_fact(spark: SparkSession, last_run: dt.datetime):
    """Xử lý missing values trong FACT và cập nhật GOLD_PRICE_FACT_CLEAN.
    Chỉ loại bỏ record thiếu critical fields, còn lại giữ nguyên.
    """
    df_fact = read_table_from_oracle(spark, "GOLD_PRICE_FACT_CLEAN", DB_USER)
    
    if df_fact.count() == 0:
        print("ℹ️ Không có dữ liệu để xử lý missing values.")
        return 0

    before_count = df_fact.count()
    
    # Chỉ loại bỏ record thiếu critical fields (BUY_PRICE, SELL_PRICE, TIME_ID)
    # Các record khác giữ nguyên để đảm bảo có đầy đủ dữ liệu
    df_clean = df_fact.filter(
        col("BUY_PRICE").isNotNull() &
        col("SELL_PRICE").isNotNull() &
        col("TIME_ID").isNotNull()
    )

    # Fill UNIT with mode
    unit_mode_row = df_clean.groupBy("UNIT").count().orderBy(col("count").desc()).first()
    unit_default = unit_mode_row[0] if unit_mode_row else "VND/Lượng"
    
    df_clean = df_clean.withColumn(
        "UNIT",
        when(col("UNIT").isNull(), lit(unit_default))
        .otherwise(col("UNIT"))
    )

    dropped = before_count - df_clean.count()

    write_table_to_oracle(df_clean, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite")
    snapshot_table(df_clean, "GOLD_PRICE_FACT_CLEAN", "after_handle_missing")
    
    print(f"🧩 Đã loại bỏ {dropped} bản ghi thiếu giá hoặc thời gian.")
    return dropped

def flag_price_outliers(spark: SparkSession, last_run: dt.datetime):
    """Phát hiện outlier giá và cập nhật GOLD_PRICE_FACT_CLEAN."""
    df_fact = read_table_from_oracle(spark, "GOLD_PRICE_FACT_CLEAN", DB_USER)
    
    if df_fact.count() == 0:
        print("ℹ️ Không có dữ liệu để flag outlier.")
        return 0

    # Calculate IQR for BUY_PRICE and SELL_PRICE using Spark
    from pyspark.sql.functions import percentile_approx
    
    # Get quantiles using Spark
    buy_q1 = df_fact.select(percentile_approx("BUY_PRICE", 0.25).alias("q1")).first()[0]
    buy_q3 = df_fact.select(percentile_approx("BUY_PRICE", 0.75).alias("q3")).first()[0]
    buy_iqr = buy_q3 - buy_q1
    buy_lower = buy_q1 - 1.5 * buy_iqr
    buy_upper = buy_q3 + 1.5 * buy_iqr
    
    sell_q1 = df_fact.select(percentile_approx("SELL_PRICE", 0.25).alias("q1")).first()[0]
    sell_q3 = df_fact.select(percentile_approx("SELL_PRICE", 0.75).alias("q3")).first()[0]
    sell_iqr = sell_q3 - sell_q1
    sell_lower = sell_q1 - 1.5 * sell_iqr
    sell_upper = sell_q3 + 1.5 * sell_iqr
    
    df_clean = df_fact.withColumn(
        "BUY_PRICE_OUTLIER",
        (col("BUY_PRICE") < lit(buy_lower)) | (col("BUY_PRICE") > lit(buy_upper))
    ).withColumn(
        "SELL_PRICE_OUTLIER",
        (col("SELL_PRICE") < lit(sell_lower)) | (col("SELL_PRICE") > lit(sell_upper))
    ).withColumn(
        "IS_DELETED",
        when((col("BUY_PRICE_OUTLIER") == True) | (col("SELL_PRICE_OUTLIER") == True), lit(1))
        .otherwise(lit(0))
    ).drop("BUY_PRICE_OUTLIER", "SELL_PRICE_OUTLIER")

    flagged = df_clean.filter(col("IS_DELETED") == 1).count()

    write_table_to_oracle(df_clean, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite")
    snapshot_table(df_clean, "GOLD_PRICE_FACT_CLEAN", "after_outlier_flag")
    
    print(f"⚠️ Đã flag {flagged} bản ghi outlier (IS_DELETED=1).")
    return flagged

# --------------------------- MAIN ---------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--merge-types", action="store_true", 
                       help="Thử gộp TYPE tương đồng về 1 ID và cập nhật FACT")
    args = parser.parse_args()

    spark = create_spark_session()

    # Snapshot trước khi xử lý
    df_loc = read_table_from_oracle(spark, "LOCATION_DIMENSION", DB_USER)
    df_type = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION", DB_USER)
    df_fact = read_table_from_oracle(spark, "GOLD_PRICE_FACT", DB_USER)
    
    # Log số lượng record gốc
    print(f"\n📊 Số lượng dữ liệu gốc:")
    print(f"   LOCATION_DIMENSION: {df_loc.count()} records")
    print(f"   GOLD_TYPE_DIMENSION: {df_type.count()} records")
    print(f"   GOLD_PRICE_FACT: {df_fact.count()} records\n")
    
    snapshot_table(df_loc, "LOCATION_DIMENSION", "before")
    snapshot_table(df_type, "GOLD_TYPE_DIMENSION", "before")
    snapshot_table(df_fact, "GOLD_PRICE_FACT", "before")

    last_run = get_last_checkpoint(spark)
    print(f"⏱️ Last checkpoint: {last_run}")

    # B1: LOCATION normalize -> LOCATION_DIMENSION_CLEAN
    location_mapping = normalize_locations(spark)

    # B2: GOLD TYPE enrich -> GOLD_TYPE_DIMENSION_CLEAN
    enrich_gold_types(spark)
    normalize_purity_format(spark)
    normalize_category_smart(spark)

    # (Tuỳ chọn) gộp TYPE tương đồng
    type_mapping = {}
    if args.merge_types:
        type_mapping = merge_duplicate_types_and_update_fact(spark)

    normalize_gold_type_and_unit(spark)

    # B3: FACT dedup incremental -> GOLD_PRICE_FACT_CLEAN
    # Đọc toàn bộ FACT và apply mappings, sau đó dedup
    df_fact_all = read_table_from_oracle(spark, "GOLD_PRICE_FACT", DB_USER)
    fact_original_count = df_fact_all.count()
    print(f"📊 GOLD_PRICE_FACT gốc: {fact_original_count} records")
    
    # Apply location mapping
    if location_mapping:
        mapping_df = spark.createDataFrame(
            [(k, v) for k, v in location_mapping.items()],
            ["OLD_LOC_ID", "NEW_LOC_ID"]
        )
        df_fact_all = df_fact_all.join(
            mapping_df,
            df_fact_all["LOCATION_ID"] == mapping_df["OLD_LOC_ID"],
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
        df_fact_all = df_fact_all.join(
            mapping_df,
            df_fact_all["TYPE_ID"] == mapping_df["OLD_TYPE_ID"],
            "left"
        ).withColumn(
            "TYPE_ID",
            when(col("NEW_TYPE_ID").isNotNull(), col("NEW_TYPE_ID"))
            .otherwise(col("TYPE_ID"))
        ).drop("OLD_TYPE_ID", "NEW_TYPE_ID")
    
    # Log số lượng sau khi apply mappings
    fact_after_mapping_count = df_fact_all.count()
    print(f"📊 GOLD_PRICE_FACT sau mapping: {fact_after_mapping_count} records")
    
    # Đảm bảo luôn có dữ liệu trong bảng CLEAN (copy toàn bộ nếu cần)
    if fact_after_mapping_count == 0 and fact_original_count > 0:
        print("⚠️ Cảnh báo: Sau mapping bảng CLEAN rỗng nhưng bảng gốc có dữ liệu! Copy toàn bộ dữ liệu gốc...")
        df_fact_all = read_table_from_oracle(spark, "GOLD_PRICE_FACT", DB_USER)
        fact_after_mapping_count = df_fact_all.count()
    
    # Write initial clean fact table (luôn có dữ liệu, kể cả không có gì để clean)
    write_table_to_oracle(df_fact_all, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite")
    print(f"✅ Đã ghi {fact_after_mapping_count} records vào GOLD_PRICE_FACT_CLEAN")
    
    # Then apply dedup and other cleaning
    dedup_fact_incremental(spark, last_run, {}, {})  # Mappings already applied
    handle_missing_values_fact(spark, last_run)
    flag_price_outliers(spark, last_run)

    # Cập nhật checkpoint
    now = dt.datetime.now()
    set_checkpoint(spark, now)
    print(f"✅ Job hoàn tất. Checkpoint mới: {now}")

    # Snapshot cuối
    df_loc_clean = read_table_from_oracle(spark, "LOCATION_DIMENSION_CLEAN", DB_USER)
    df_type_clean = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION_CLEAN", DB_USER)
    df_fact_clean = read_table_from_oracle(spark, "GOLD_PRICE_FACT_CLEAN", DB_USER)
    
    snapshot_table(df_loc_clean, "LOCATION_DIMENSION_CLEAN", "final")
    snapshot_table(df_type_clean, "GOLD_TYPE_DIMENSION_CLEAN", "final")
    snapshot_table(df_fact_clean, "GOLD_PRICE_FACT_CLEAN", "final")

    spark.stop()

if __name__ == "__main__":
    # Có thể chạy trực tiếp với Python: python daily_gold_job_normalization_spark.py
    # Hoặc dùng script helper: python run_spark_job_local.py
    main()

