# -*- coding: utf-8 -*-
"""
Daily Gold ETL Job (Oracle 23ai schema)
- B1: LOCATION_DIMENSION: phát hiện thành phố trùng ngữ nghĩa -> gộp về 1 ID và cập nhật GOLD_PRICE_FACT.LOCATION_ID
- B2: GOLD_TYPE_DIMENSION: dùng tương đồng để điền PURITY/CATEGORY còn thiếu; (tuỳ chọn) gộp TYPE giống nhau -> cập nhật FACT.TYPE_ID
- B3: GOLD_PRICE_FACT: với (SOURCE_ID, TYPE_ID, LOCATION_ID, TIME_ID) trùng nhau -> giữ RECORDED_AT mới nhất, còn lại IS_DELETED=1
- Incremental bằng checkpoint trong DB (bảng ETL_CHECKPOINT)
- Chụp snapshot trước/sau ra CSV để báo cáo
- Tuỳ chọn insert dữ liệu fake (đa nguồn, synonym địa danh & gold type) đúng với schema bạn gửi
"""

import argparse
import datetime as dt
import os
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import oracledb

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from fuzzywuzzy import fuzz

import re

# ====================== CONFIG ======================
DB_USER = "ADMIN"
DB_PASS = "Abcd12345678!"
DB_DSN  = "34.126.123.190:1521/MYATP_low.adb.oraclecloud.com"  # giữ nguyên DSN bạn đưa

SNAPSHOT_DIR = "./snapshots"  # nơi lưu CSV trước/sau để chụp màn hình báo cáo
JOB_NAME = "DAILY_GOLD_JOB"
SIM_THRESHOLD_LOC = 0.80      # ngưỡng cosine TF-IDF cho City
SIM_THRESHOLD_TYPE = 0.75     # ngưỡng cosine TF-IDF cho TypeName
FUZZY_FALLBACK = 90           # ngưỡng fuzzy token_set_ratio fallback

# ====================================================

def conn():
    return oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)

def ensure_infra(c):
    """Đảm bảo các bảng/cột phụ trợ tồn tại (ETL_CHECKPOINT, IS_DELETE nếu chưa có)."""
    with c.cursor() as cur:
        # 1) Bảng checkpoint
        cur.execute("""
            BEGIN
                EXECUTE IMMEDIATE '
                    CREATE TABLE ETL_CHECKPOINT (
                        JOB_NAME VARCHAR2(100) PRIMARY KEY,
                        LAST_RUN TIMESTAMP
                    )
                ';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN RAISE; END IF; -- -955: table already exists
            END;""")

        # 2) Cột IS_DELETE trong GOLD_PRICE_FACT (schema đã có IS_DELETED; ta vẫn thêm IS_DELETE nếu chưa có để tương thích job cũ)
        cur.execute("""
            DECLARE
                v_dummy NUMBER;
            BEGIN
                SELECT 1 INTO v_dummy FROM USER_TAB_COLS 
                WHERE TABLE_NAME = 'GOLD_PRICE_FACT' AND COLUMN_NAME = 'IS_DELETE';
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    EXECUTE IMMEDIATE 'ALTER TABLE GOLD_PRICE_FACT ADD (IS_DELETE NUMBER(1) DEFAULT 0)';
            END;""")

    c.commit()

def get_last_checkpoint(c) -> dt.datetime:
    with c.cursor() as cur:
        cur.execute("SELECT LAST_RUN FROM ETL_CHECKPOINT WHERE JOB_NAME = :j", {"j": JOB_NAME})
        row = cur.fetchone()
        if not row or not row[0]:
            return dt.datetime(2000,1,1)  # lùi sâu lần đầu
        return row[0]

def set_checkpoint(c, ts: dt.datetime):
    with c.cursor() as cur:
        cur.execute("""
            MERGE INTO ETL_CHECKPOINT t
            USING (SELECT :j JOB_NAME, :lr LAST_RUN FROM dual) s
            ON (t.JOB_NAME = s.JOB_NAME)
            WHEN MATCHED THEN UPDATE SET t.LAST_RUN = s.LAST_RUN
            WHEN NOT MATCHED THEN INSERT (JOB_NAME, LAST_RUN) VALUES (s.JOB_NAME, s.LAST_RUN)
        """, {"j": JOB_NAME, "lr": ts})
    c.commit()

def snapshot_table(c, table: str, tag: str):
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    df = pd.read_sql(f'SELECT * FROM "{DB_USER}"."{table}"', c)  # giữ schema ADMIN tường minh
    path = os.path.join(SNAPSHOT_DIR, f"{table}_{tag}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"📸 Snapshot {table} -> {path}")

# ----------------------- Fake data -----------------------

def get_or_create_time_id(c, date_obj: dt.date) -> int:
    """Trả về TIME_DIMENSION.ID tương ứng với DATE_TIME = date_obj (00:00:00). Nếu chưa có thì tạo mới."""
    with c.cursor() as cur:
        cur.execute('SELECT ID FROM TIME_DIMENSION WHERE TRUNC(DATE_TIME)=:d', {"d": date_obj})
        r = cur.fetchone()
        if r:
            return int(r[0])
        # chèn mới
        cur.execute(
            'INSERT INTO TIME_DIMENSION (DATE_TIME, YEAR, MONTH, DAY, HOUR) VALUES (:dt, :y, :m, :d, :h) RETURNING ID INTO :rid',
            {"dt": dt.datetime.combine(date_obj, dt.time(0,0,0)), "y": date_obj.year, "m": date_obj.month, "d": date_obj.day, "h": 0, "rid": cur.var(oracledb.NUMBER)}
        )
        rid = int(cur.getimplicitresults()[0][0]) if hasattr(cur, "getimplicitresults") else None
        if rid is None:
            # fallback: lấy lại
            cur.execute('SELECT ID FROM TIME_DIMENSION WHERE TRUNC(DATE_TIME)=:d', {"d": date_obj})
            rid = int(cur.fetchone()[0])
        c.commit()
        return rid

def insert_fake_data(c):
    """Tạo dữ liệu demo theo đúng schema (ID, TYPE_NAME, CITY, ...)."""
    with c.cursor() as cur:
        # LOCATION_DIMENSION demo (chèn tên đồng nghĩa)
        for (idv, city, region) in [
            (101, 'Hồ Chí Minh', 'Miền Nam'),
            (102, 'Sài Gòn', 'Miền Nam'),
            (201, 'Hà Nội', 'Miền Bắc'),
            (202, 'Thủ đô', 'Miền Bắc'),
            (301, 'Đà Nẵng', 'Miền Trung'),
            (302, 'Danang', 'Miền Trung'),
        ]:
            try:
                cur.execute('INSERT INTO LOCATION_DIMENSION (ID, CITY, REGION) VALUES (:i, :c, :r)', {"i": idv, "c": city, "r": region})
            except oracledb.Error:
                pass  # đã tồn tại

        # GOLD_TYPE_DIMENSION demo
        for (idv, tname, purity, cat, brand) in [
            (11, 'Vàng SJC 1L', '99.99%', 'Gold bar', 'SJC'),
            (12, 'Vàng SJC 5 chỉ', None, 'other', 'SJC'),
            (13, 'Vàng SJC 10L', '99.99%', 'Gold bar', 'SJC'),
            (14, 'Phú Quý 1 lượng 99.9', '99.9%', 'other', 'Phú Quý'),
            (15, 'Phú Quý 5 chỉ', None, None, 'Phú Quý'),
        ]:
            try:
                cur.execute('INSERT INTO GOLD_TYPE_DIMENSION (ID, TYPE_NAME, PURITY, CATEGORY, BRAND) VALUES (:i,:t,:p,:c,:b)',
                            {"i": idv, "t": tname, "p": purity, "c": cat, "b": brand})
            except oracledb.Error:
                pass

        # SOURCE_DIMENSION demo
        for (idv, name, url, descp) in [
            (1, 'PNJ',  'https://www.giavang.pnj.com.vn/', 'PNJ'),
            (2, 'DOJI', 'https://www.giadoji.vn/', 'DOJI'),
        ]:
            try:
                cur.execute('INSERT INTO SOURCE_DIMENSION (ID, SOURCE_NAME, SOURCE_URL, DESCRIPTION) VALUES (:i,:n,:u,:d)',
                            {"i": idv, "n": name, "u": url, "d": descp})
            except oracledb.Error:
                pass

        # TIME_DIMENSION demo (10 ngày gần đây)
        for d in pd.date_range(end=pd.Timestamp.today().normalize(), periods=10):
            _ = get_or_create_time_id(c, d.date())

        # GOLD_PRICE_FACT demo: cố tình trùng lặp & mix location/type synonym, nhiều nguồn
        base = [
            (1, 11, 101), (1, 12, 102), (1, 13, 201),
            (2, 14, 202), (2, 15, 301), (1, 11, 302)
        ]
        now = pd.Timestamp.now()
        pid = 50000
        for (src, typ, loc) in base:
            for d in pd.date_range(end=pd.Timestamp.today().normalize(), periods=5):
                time_id = get_or_create_time_id(c, d.date())
                buy = 70000000 + np.random.randint(-300000, 300000)
                sell = buy + np.random.randint(50000, 200000)
                # chèn 2 bản ghi cùng key nhưng RECORDED_AT khác nhau
                for dup in range(2):
                    try:
                        cur.execute("""
                            INSERT INTO GOLD_PRICE_FACT
                                (ID, SOURCE_ID, TYPE_ID, LOCATION_ID, TIME_ID,
                                 BUY_PRICE, SELL_PRICE, UNIT, RECORDED_AT, IS_DELETED, RECORDED_BY, IS_DELETE)
                            VALUES
                                (:id, :sid, :tid, :lid, :tt,
                                 :bp, :sp, 'VND/Lượng', :rec, 0, 'demo', 0)
                        """, {
                            "id": pid, "sid": src, "tid": typ, "lid": loc, "tt": time_id,
                            "bp": buy, "sp": sell,
                            "rec": (now - pd.Timedelta(days=5 - (pd.Timestamp.today().normalize() - d).days, minutes=10 - dup)).to_pydatetime()
                        })
                        pid += 1
                    except oracledb.Error:
                        pass

    c.commit()
    print("✅ Đã chèn dữ liệu demo.")

# -------------------- LOCATION normalize --------------------

import unicodedata

# ======= Bộ từ điển đồng nghĩa & chống gộp =======

def norm_txt(s: str) -> str:
    """Chuẩn hoá tiếng Việt (bỏ dấu, lowercase, trim) để so khớp ổn định."""
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s

# Cặp ép gộp (ép buộc nhóm cùng)
POSITIVE_SYNONYMS = {
    ("ho chi minh", "tphcm"),
    ("ho chi minh", "tp hcm"),
    ("da nang", "danang"),
    ("ha noi", "thu do"),
    ("ha noi", "hn"),
}

# Cặp cấm gộp (kể cả similarity cao vẫn không gộp)
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

# Tính TF-IDF (char n-gram) cho từng tên.
# Dùng cosine_similarity để đo độ giống nhau giữa các tên.
# Nếu cosine >= SIM_THRESHOLD_LOC (0.75) hoặc fuzz.token_set_ratio >= 90, coi là tương đồng.
# Có bộ luật ép gộp và bộ luật cấm gộp:
# Ví dụ: ép gộp “Đà Nẵng” ~ “Danang”.
# Cấm gộp “Sài Gòn” ~ “Hồ Chí Minh”.
def build_similarity_groups(values: List[str], threshold: float) -> List[List[int]]:
    """Nhóm các index có độ tương đồng cosine TF-IDF >= threshold + fallback fuzzy, có ép gộp & cấm gộp."""
    if not values:
        return []

    vec = TfidfVectorizer(ngram_range=(1,2), analyzer='char_wb').fit(values)
    tf = vec.transform(values)
    sim = cosine_similarity(tf)

    n = len(values)
    visited = [False]*n
    groups = []

    for i in range(n):
        if visited[i]:
            continue
        group = [i]
        visited[i] = True

        for j in range(i+1, n):
            if visited[j]:
                continue

            a, b = values[i], values[j]

            # 1️⃣ Nếu bị cấm gộp → bỏ qua
            if pair_blocked(a, b):
                print(f"🚫 BLOCKED: '{a}' vs '{b}'")
                continue

            # 2️⃣ Nếu là cặp ép gộp → gộp luôn
            if pair_forced(a, b):
                print(f"✅ FORCED GROUP: '{a}' ~ '{b}'")
                group.append(j)
                visited[j] = True
                continue

            # 3️⃣ Nếu similarity đủ cao → gộp
            if sim[i, j] >= threshold or fuzz.token_set_ratio(a, b) >= FUZZY_FALLBACK:
                print(f"≈ SIMILAR: '{a}' ~ '{b}' (cos={sim[i,j]:.2f}, fuzzy={fuzz.token_set_ratio(a,b)})")
                group.append(j)
                visited[j] = True

        groups.append(group)
    return groups

# Trong bảng LOCATION_DIMENSION, có thể có nhiều bản ghi cùng nghĩa nhưng khác cách viết (ví dụ: “Hồ Chí Minh”, “TP HCM”, “Sài Gòn”, “Ho Chi Minh City”…).
# Hàm này:
# Phát hiện các thành phố tương tự nhau (ngữ nghĩa gần nhau).
# Gộp về cùng một LOCATION_ID chuẩn (canonical ID).
# Cập nhật bảng GOLD_PRICE_FACT để tất cả bản ghi cùng nghĩa dùng LOCATION_ID thống nhất.
def normalize_locations(c) -> Dict[int, int]:
    """Phát hiện & gộp các LOCATION tương đồng; có snapshot trước/sau để báo cáo."""
    df_loc = pd.read_sql('SELECT ID AS LOCATION_ID, CITY FROM LOCATION_DIMENSION', c)
    if df_loc.empty:
        print("⚠️ LOCATION_DIMENSION trống.")
        return {}

    # Snapshot trước khi xử lý
    snapshot_table(c, "LOCATION_DIMENSION", "before_loc_norm")

    names = df_loc["CITY"].astype(str).fillna("").str.lower().tolist()
    groups = build_similarity_groups(names, SIM_THRESHOLD_LOC)

    mapping = {}
    for grp in groups:
        ids = df_loc.iloc[grp]["LOCATION_ID"].tolist()
        canon = min(ids)
        for idx in grp:
            lid = int(df_loc.iloc[idx]["LOCATION_ID"])
            if lid != canon:
                mapping[lid] = canon

    print(f"🔎 Mapping location (old->new): {mapping}")

    # Cập nhật vào FACT
    with c.cursor() as cur:
        for old_id, new_id in mapping.items():
            cur.execute(
                "UPDATE GOLD_PRICE_FACT SET LOCATION_ID = :new WHERE LOCATION_ID = :old",
                {"new": new_id, "old": old_id}
            )
    c.commit()

    snapshot_table(c, "LOCATION_DIMENSION", "after_loc_norm")
    return mapping


# -------------------- GOLD TYPE enrichment --------------------
# Dùng để tự động điền thông tin còn thiếu (PURITY, CATEGORY) trong bảng 
#GOLD_TYPE_DIMENSION dựa trên sự tương đồng của tên loại vàng (TYPE_NAME).
# Dùng TF-IDF + Cosine Similarity (scikit-learn) và FuzzyWuzzy để tìm các loại vàng gần giống nhau.
# Trong mỗi nhóm tương đồng, lấy giá trị phổ biến nhất (mode) của PURITY và CATEGORY để điền cho bản ghi bị thiếu.
def enrich_gold_types(c) -> Tuple[int, int]:
    """
    Làm giàu GOLD_TYPE_DIMENSION: 
    - Điền PURITY/CATEGORY còn thiếu dựa vào nhóm tương tự TYPE_NAME.
    - Bổ sung fallback nếu nhóm toàn None.
    - Ghi snapshot before/after và in log chi tiết thay đổi.
    """
    df = pd.read_sql('SELECT ID AS TYPE_ID, TYPE_NAME, PURITY, CATEGORY FROM GOLD_TYPE_DIMENSION', c)
    if df.empty:
        print("⚠️ GOLD_TYPE_DIMENSION trống.")
        return (0, 0)

    snapshot_table(c, "GOLD_TYPE_DIMENSION", "before_type_enrich")

    values = df["TYPE_NAME"].astype(str).str.lower().fillna("").tolist()
    groups = build_similarity_groups(values, SIM_THRESHOLD_TYPE)

    purity_fill = 0
    category_fill = 0
    with c.cursor() as cur:
        for grp in groups:
            sub = df.iloc[grp]

            # Lấy mode của các giá trị khác "unknown"/None
            known_purity = sub["PURITY"].dropna()
            known_purity = known_purity[~known_purity.astype(str).str.lower().isin(["unknown", "nan", "none", ""])]
            known_cat = sub["CATEGORY"].dropna()
            known_cat = known_cat[~known_cat.astype(str).str.lower().isin(["unknown", "nan", "none", "other", ""])]

            purity_mode = known_purity.mode().iloc[0] if not known_purity.empty else "99.99%"
            cat_mode = known_cat.mode().iloc[0] if not known_cat.empty else "Gold bar"

            for _, row in sub.iterrows():
                tid = int(row["TYPE_ID"])

                # ---- fill PURITY ----
                purity = str(row["PURITY"]).strip().lower() if row["PURITY"] is not None else ""
                if purity in ["", "unknown", "nan", "none"]:
                    cur.execute("UPDATE GOLD_TYPE_DIMENSION SET PURITY = :p WHERE ID = :id",
                                {"p": purity_mode, "id": tid})
                    purity_fill += 1

                # ---- fill CATEGORY ----
                cat = str(row["CATEGORY"]).strip().lower() if row["CATEGORY"] is not None else ""
                if cat in ["", "unknown", "nan", "none", "other"]:
                    cur.execute("UPDATE GOLD_TYPE_DIMENSION SET CATEGORY = :c WHERE ID = :id",
                                {"c": cat_mode, "id": tid})
                    category_fill += 1

    c.commit()
    snapshot_table(c, "GOLD_TYPE_DIMENSION", "after_type_enrich")

    print(f"✨ Đã fill PURITY: {purity_fill}, CATEGORY: {category_fill}")

    # ==== So sánh before/after để xem thay đổi ====
    df_before = pd.read_csv(
        os.path.join(SNAPSHOT_DIR, sorted([f for f in os.listdir(SNAPSHOT_DIR) if "GOLD_TYPE_DIMENSION_before_type_enrich" in f])[-1]),
        encoding="utf-8-sig"
    )
    df_after = pd.read_csv(
        os.path.join(SNAPSHOT_DIR, sorted([f for f in os.listdir(SNAPSHOT_DIR) if "GOLD_TYPE_DIMENSION_after_type_enrich" in f])[-1]),
        encoding="utf-8-sig"
    )

    compare = df_before.merge(df_after, on="ID", suffixes=("_BEFORE", "_AFTER"))
    changed = compare[
        (compare["PURITY_BEFORE"] != compare["PURITY_AFTER"]) |
        (compare["CATEGORY_BEFORE"] != compare["CATEGORY_AFTER"])
    ]

    if not changed.empty:
        path_diff = os.path.join(SNAPSHOT_DIR, f"GOLD_TYPE_DIMENSION_diff_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        changed.to_excel(path_diff, index=False)
        print(f"📊 Đã tạo file so sánh Before/After: {path_diff}")
        for _, row in changed.iterrows():
            print(f" - {row['TYPE_NAME_BEFORE']}: PURITY {row['PURITY_BEFORE']} → {row['PURITY_AFTER']}, CATEGORY {row['CATEGORY_BEFORE']} → {row['CATEGORY_AFTER']}")
    else:
        print("ℹ️ Không có thay đổi nào trong PURITY hoặc CATEGORY.")

    return (purity_fill, category_fill)


# Loại bỏ các kiểu nhập liệu không đồng nhất như:
# "99,99", "99.9%", "99.99 % ", "99,9", "99.9"…
# Đưa tất cả về dạng chuẩn “99.99%” để dễ so sánh và xử lý sau này.
def normalize_purity_format(c):
    """
    Chuẩn hoá cột PURITY trong GOLD_TYPE_DIMENSION về dạng 'xx.xx%'.
    Ví dụ: '99,99', '99.99', '99.99 %', '99.9', '99,9%' -> '99.99%'
    """
    df = pd.read_sql('SELECT ID, PURITY FROM GOLD_TYPE_DIMENSION', c)
    if df.empty:
        print("⚠️ GOLD_TYPE_DIMENSION trống.")
        return 0

    changed = []
    with c.cursor() as cur:
        for _, row in df.iterrows():
            old_val = str(row["PURITY"]).strip() if row["PURITY"] is not None else ""
            new_val = old_val

            # Bỏ ký tự %, khoảng trắng, đổi dấu phẩy sang chấm
            new_val = new_val.replace("%", "").replace(" ", "").replace(",", ".").lower()

            # Loại bỏ chuỗi không hợp lệ
            if new_val in ["", "none", "nan", "unknown", "unk"]:
                continue

            # Lấy phần số
            nums = re.findall(r"[\d\.]+", new_val)
            if not nums:
                continue

            try:
                val = float(nums[0])
                if val <= 0 or val > 100:
                    continue
                new_val = f"{val:.2f}%"
            except ValueError:
                continue

            if new_val != old_val:
                cur.execute("UPDATE GOLD_TYPE_DIMENSION SET PURITY = :p WHERE ID = :i", {"p": new_val, "i": int(row["ID"])})
                changed.append((row["ID"], old_val, new_val))

    c.commit()

    print(f"🔧 Đã chuẩn hoá PURITY cho {len(changed)} bản ghi:")
    for cid, old, new in changed[:10]:
        print(f" - ID {cid}: '{old}' → '{new}'")
    if len(changed) > 10:
        print(f"   ... và {len(changed)-10} bản ghi khác.")
    return len(changed)


def merge_duplicate_types_and_update_fact(c):
    """
    Gộp các bản ghi GOLD_TYPE_DIMENSION trùng 4 cột (TYPE_NAME, PURITY, CATEGORY, BRAND):
    - Giữ ID nhỏ nhất làm chuẩn.
    - Cập nhật GOLD_PRICE_FACT.TYPE_ID về ID chuẩn.
    - Gắn IS_DELETED=1 cho TYPE_ID cũ đã được gộp.
    """
    df = pd.read_sql(
        'SELECT ID AS TYPE_ID, TYPE_NAME, PURITY, CATEGORY, BRAND FROM GOLD_TYPE_DIMENSION', c
    )
    if df.empty:
        print("⚠️ GOLD_TYPE_DIMENSION trống.")
        return

    # Chuẩn hoá dữ liệu để tránh lệch
    for col in ["TYPE_NAME", "PURITY", "CATEGORY", "BRAND"]:
        df[col] = df[col].astype(str).fillna("").str.strip().str.lower()

    grouped = df.groupby(["TYPE_NAME", "PURITY", "CATEGORY", "BRAND"])
    mapping = {}

    for (tname, purity, cat, brand), subdf in grouped:
        ids = subdf["TYPE_ID"].tolist()
        if len(ids) <= 1:
            continue
        canon = min(ids)
        dups = [tid for tid in ids if tid != canon]
        for tid in dups:
            mapping[tid] = canon
        print(f"✅ Gộp '{tname}' | '{purity}' | '{cat}' | '{brand}' → giữ ID {canon}, gộp {dups}")

    if not mapping:
        print("ℹ️ Không có TYPE trùng cần gộp.")
        return

    # --- 1️⃣ Cập nhật FACT.TYPE_ID ---
    with c.cursor() as cur:
        for old_id, new_id in mapping.items():
            cur.execute("""
                UPDATE GOLD_PRICE_FACT
                SET TYPE_ID = :new
                WHERE TYPE_ID = :old
            """, {"new": new_id, "old": old_id})
    c.commit()
    print(f"🔁 Đã cập nhật GOLD_PRICE_FACT.TYPE_ID cho {len(mapping)} bản ghi trùng.")

    # --- 2️⃣ Đảm bảo cột IS_DELETED tồn tại ---
    with c.cursor() as cur:
        cur.execute("""
            DECLARE v NUMBER;
            BEGIN
              SELECT 1 INTO v FROM USER_TAB_COLS 
              WHERE TABLE_NAME='GOLD_TYPE_DIMENSION' AND COLUMN_NAME='IS_DELETED';
            EXCEPTION WHEN NO_DATA_FOUND THEN
              EXECUTE IMMEDIATE 'ALTER TABLE GOLD_TYPE_DIMENSION ADD (IS_DELETED NUMBER(1) DEFAULT 0)';
            END;
        """)

        # --- 3️⃣ Gắn IS_DELETED=1 cho TYPE_ID cũ đã bị gộp ---
        for old_id in mapping.keys():
            cur.execute("""
                UPDATE GOLD_TYPE_DIMENSION
                SET IS_DELETED = 1
                WHERE ID = :old_id
            """, {"old_id": old_id})
    c.commit()
    print(f"🧹 Đã gắn IS_DELETED=1 cho {len(mapping)} TYPE_ID cũ đã bị gộp.")




def normalize_text(s: str) -> str:
    """Chuẩn hoá text dạng lowercase, bỏ ký tự đặc biệt, sắp xếp từ a-z để gộp 'bar gold' = 'gold bar'"""
    s = s or ""
    s = re.sub(r'[^A-Za-z0-9]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip().lower()
    # Sắp xếp từ để gộp các đảo thứ tự (vd: gold bar, bar gold)
    tokens = sorted(s.split())
    return " ".join(tokens)

def title_case(s: str) -> str:
    """Viết hoa chữ đầu mỗi từ"""
    return s.title()

def normalize_category_smart(c):
    """
    Chuẩn hoá CATEGORY:
    - Gộp biến thể (gold bar, gold_bar, GOLD-BAR, bar gold, gold   bar,...)
    - Chọn dạng chuẩn viết hoa đầu từ (Gold Bar)
    - Cập nhật trực tiếp vào DB
    """
    df = pd.read_sql('SELECT ID, CATEGORY FROM GOLD_TYPE_DIMENSION', c)
    if df.empty:
        print("⚠️ GOLD_TYPE_DIMENSION trống.")
        return 0

    # Chuẩn hoá & nhóm tương đồng
    df["CLEAN"] = df["CATEGORY"].astype(str).apply(normalize_text)
    unique_vals = df["CLEAN"].unique().tolist()

    # Gom nhóm theo độ tương đồng fuzzy
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

    # Xác định giá trị chuẩn của mỗi nhóm
    mapping = {}
    for grp in groups:
        canon = sorted(grp, key=len)[0]  # lấy chuỗi ngắn nhất làm chuẩn
        for val in grp:
            mapping[val] = canon

    # Cập nhật DB
    changed = []
    with c.cursor() as cur:
        for _, row in df.iterrows():
            old_raw = row["CATEGORY"]
            clean = row["CLEAN"]
            canon_clean = mapping.get(clean, clean)
            new_val = title_case(canon_clean)
            if new_val != old_raw:
                cur.execute("UPDATE GOLD_TYPE_DIMENSION SET CATEGORY = :c WHERE ID = :i",
                            {"c": new_val, "i": int(row["ID"])})
                changed.append((row["ID"], old_raw, new_val))
        c.commit()

    print(f"✅ Đã chuẩn hoá CATEGORY cho {len(changed)} bản ghi:")
    for cid, old, new in changed[:10]:
        print(f" - ID {cid}: '{old}' → '{new}'")
    if len(changed) > 10:
        print(f"   ... và {len(changed) - 10} bản ghi khác.")
    return len(changed)




# -------------------- FACT dedup incremental --------------------

def dedup_fact_incremental(c, last_run: dt.datetime):
    # chừa biên 1 ngày để an toàn
    floor_ts = last_run - dt.timedelta(days=1)
    q = """
        SELECT ID, SOURCE_ID, TYPE_ID, LOCATION_ID, TIME_ID, RECORDED_AT, IS_DELETED
        FROM GOLD_PRICE_FACT
        WHERE RECORDED_AT >= :ts
    """
    df = pd.read_sql(q, c, params={"ts": floor_ts})
    if df.empty:
        print("ℹ️ Không có FACT mới để dedup.")
        return 0

    # combo key 4 trường
    df["COMBO"] = (
        df["SOURCE_ID"].astype(str) + "_" +
        df["TYPE_ID"].astype(str) + "_" +
        df["LOCATION_ID"].astype(str) + "_" +
        df["TIME_ID"].astype(str)
    )
    # giữ bản mới nhất theo RECORDED_AT
    keep_idx = df.sort_values(["COMBO", "RECORDED_AT"]).groupby("COMBO").tail(1).index
    to_mark = df.index.difference(keep_idx)
    n_dup = len(to_mark)

    if n_dup > 0:
        with c.cursor() as cur:
            ids = df.loc[to_mark, "ID"].astype(int).tolist()
            for pid in ids:
                cur.execute("UPDATE GOLD_PRICE_FACT SET IS_DELETED = 1, IS_DELETE = 1 WHERE ID = :p", {"p": pid})
        c.commit()

    snapshot_table(c, "GOLD_PRICE_FACT", "after_fact_dedup")
    print(f"🧹 Đã gắn IS_DELETED/IS_DELETE=1 cho {n_dup} bản ghi trùng.")
    return n_dup

    
def handle_missing_values_fact(c, last_run: dt.datetime):
    """
    Xử lý missing values (incremental):
    - Chỉ kiểm tra dữ liệu mới hoặc cập nhật sau last_run.
    - Drop bản ghi thiếu BUY_PRICE, SELL_PRICE, TIME_ID.
    - Impute UNIT bằng mode.
    - Flag IS_DELETED=1 cho bản ghi không hợp lệ.
    """
    floor_ts = last_run - dt.timedelta(days=1)
    q = """
        SELECT ID, BUY_PRICE, SELL_PRICE, TIME_ID, UNIT, RECORDED_AT
        FROM GOLD_PRICE_FACT
        WHERE RECORDED_AT >= :ts
    """
    df = pd.read_sql(q, c, params={"ts": floor_ts})
    if df.empty:
        print("ℹ️ Không có dữ liệu mới để xử lý missing values.")
        return 0

    before = len(df)
    df_clean = df.dropna(subset=["BUY_PRICE", "SELL_PRICE", "TIME_ID"])

    if "UNIT" in df_clean.columns:
        mode_val = df_clean["UNIT"].mode().iloc[0] if not df_clean["UNIT"].isna().all() else "VND/Lượng"
        df_clean["UNIT"] = df_clean["UNIT"].fillna(mode_val)

    dropped = before - len(df_clean)
    dropped_ids = df.loc[df.index.difference(df_clean.index), "ID"].astype(int).tolist()

    if dropped_ids:
        with c.cursor() as cur:
            for pid in dropped_ids:
                cur.execute("UPDATE GOLD_PRICE_FACT SET IS_DELETED = 1, IS_DELETE = 1 WHERE ID = :p", {"p": pid})
        c.commit()

    snapshot_table(c, "GOLD_PRICE_FACT", "after_handle_missing")
    print(f"🧩 Đã flag {dropped} bản ghi thiếu giá hoặc thời gian (IS_DELETED=1).")
    return dropped




def flag_price_outliers(c, last_run: dt.datetime):
    """
    Phát hiện outlier giá mua/bán (incremental):
    - Dựa trên IQR method.
    - Chỉ xử lý dữ liệu mới hoặc cập nhật sau last_run.
    - Flag IS_DELETED=1 cho các bản ghi vượt ngưỡng.
    """
    floor_ts = last_run - dt.timedelta(days=1)
    q = """
        SELECT ID, BUY_PRICE, SELL_PRICE, RECORDED_AT
        FROM GOLD_PRICE_FACT
        WHERE RECORDED_AT >= :ts
    """
    df = pd.read_sql(q, c, params={"ts": floor_ts})
    if df.empty:
        print("ℹ️ Không có dữ liệu mới để flag outlier.")
        return 0

    for col in ["BUY_PRICE", "SELL_PRICE"]:
        q1, q3 = df[col].quantile([0.25, 0.75])
        iqr = q3 - q1
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        df[f"{col}_OUTLIER"] = ((df[col] < lower) | (df[col] > upper)).astype(int)

    df["IS_DELETED"] = ((df["BUY_PRICE_OUTLIER"] == 1) | (df["SELL_PRICE_OUTLIER"] == 1)).astype(int)
    flagged = df["IS_DELETED"].sum()

    with c.cursor() as cur:
        cur.execute("""
            DECLARE
                v_dummy NUMBER;
            BEGIN
                SELECT 1 INTO v_dummy FROM USER_TAB_COLS 
                WHERE TABLE_NAME='GOLD_PRICE_FACT' AND COLUMN_NAME='IS_DELETED';
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    EXECUTE IMMEDIATE 'ALTER TABLE GOLD_PRICE_FACT ADD (IS_DELETED NUMBER(1) DEFAULT 0)';
            END;
        """)
        for _, r in df[df["IS_DELETED"] == 1].iterrows():
            cur.execute("UPDATE GOLD_PRICE_FACT SET IS_DELETED = 1 WHERE ID = :i", {"i": int(r["ID"])})

    c.commit()
    snapshot_table(c, "GOLD_PRICE_FACT", "after_outlier_flag")
    print(f"⚠️ Đã flag {flagged} bản ghi outlier mới (IS_DELETED=1).")
    return flagged




def normalize_gold_type_and_unit(c):
    """
    Chuẩn hóa thương hiệu và đơn vị đo trong dữ liệu vàng.
    - Chuẩn hóa BRAND: bỏ ký tự thừa, viết hoa.
    - Chuẩn hóa UNIT: quy về 'VND/Lượng'.
    """
    with c.cursor() as cur:
        # BRAND normalization
        cur.execute("""
            UPDATE GOLD_TYPE_DIMENSION 
            SET BRAND = UPPER(TRIM(REPLACE(REPLACE(BRAND, '.', ''), 'VÀNG ', '')))
            WHERE BRAND IS NOT NULL
        """)

        # UNIT normalization
        cur.execute("""
            UPDATE GOLD_PRICE_FACT 
            SET UNIT = 'VND/Lượng'
            WHERE UNIT IN ('VND/chi', 'VND/chỉ', 'triệu/lượng', 'VND/gram', 'VND/Gr', 'VND/luong')
        """)

    c.commit()
    snapshot_table(c, "GOLD_PRICE_FACT", "after_unit_norm")
    print("📏 Đã chuẩn hóa BRAND và UNIT về dạng chuẩn.")



# --------------------------- MAIN ---------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--merge-types", action="store_true", help="Thử gộp TYPE tương đồng về 1 ID và cập nhật FACT") 
    args = parser.parse_args()

    c = conn()
    ensure_infra(c)

    # Snapshot trước khi xử lý để chụp "before"
    snapshot_table(c, "LOCATION_DIMENSION", "before")
    snapshot_table(c, "GOLD_TYPE_DIMENSION", "before")
    snapshot_table(c, "GOLD_PRICE_FACT", "before")

    last_run = get_last_checkpoint(c)
    print(f"⏱️ Last checkpoint: {last_run}")

    # B1: LOCATION normalize
    normalize_locations(c)

    # B2: GOLD TYPE enrich
    enrich_gold_types(c)

    # B2.1: Chuẩn hoá PURITY về dạng chuẩn xx.xx%
    normalize_purity_format(c)
    normalize_category_smart(c)

    # (Tuỳ chọn) gộp TYPE tương đồng và cập nhật FACT
    if args.merge_types:
        merge_duplicate_types_and_update_fact(c)

    handle_missing_values_fact(c,last_run)
    flag_price_outliers(c,last_run)
    normalize_gold_type_and_unit(c)
    # B3: FACT dedup incremental
    dedup_fact_incremental(c, last_run)

    # cập nhật checkpoint
    now = dt.datetime.now()
    set_checkpoint(c, now)
    print(f"✅ Job hoàn tất. Checkpoint mới: {now}")

    # Snapshot cuối để chụp "after"
    snapshot_table(c, "LOCATION_DIMENSION", "final")
    snapshot_table(c, "GOLD_TYPE_DIMENSION", "final")
    snapshot_table(c, "GOLD_PRICE_FACT", "final")

    c.close()

if __name__ == "__main__":
    import time
    import datetime as dt

    while True:
        now = dt.datetime.now()
        run_time = dt.time(7, 0, 0)  # chạy mỗi sáng lúc 7:00

        # --- Chạy ngay lần đầu ---
        print(f"🚀 Lần đầu chạy job lúc {now}")
        try:
            main()
        except Exception as e:
            print(f"❌ Lỗi khi chạy job lần đầu: {e}")

        # --- Sau đó chờ đến sáng hôm sau ---
        while True:
            now = dt.datetime.now()
            if now.time().hour == run_time.hour and now.time().minute == run_time.minute:
                print(f"⏰ {now} - Bắt đầu chạy job buổi sáng...")
                try:
                    main()
                except Exception as e:
                    print(f"❌ Lỗi khi chạy job buổi sáng: {e}")
                # Sau khi chạy, chờ 24 tiếng (để không chạy lại cùng ngày)
                time.sleep(24 * 3600)
            else:
                # kiểm tra lại sau mỗi 5 phút
                time.sleep(300)