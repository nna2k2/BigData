import requests
import json
from datetime import datetime, timedelta
from tqdm import tqdm
import os

# 🗓️ Khoảng thời gian tổng thể (cập nhật khi cần)
START_DATE = datetime(2022, 10, 1)
END_DATE = datetime(2022, 10, 8)

# 📁 File lưu dữ liệu và metadata
OUTPUT_FILE = "pnj_gold_price_history_2022_2025.json"
META_FILE = "pnj_gold_meta.json"

# 🔄 Đọc dữ liệu cũ (nếu có)
if os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        all_data = json.load(f)
else:
    all_data = []

# 📅 Xác định ngày bắt đầu mới (từ metadata nếu có)
if os.path.exists(META_FILE):
    with open(META_FILE, "r", encoding="utf-8") as f:
        meta = json.load(f)
    last_date_str = meta.get("last_crawled_date")
    if last_date_str:
        start_date = datetime.strptime(last_date_str, "%Y%m%d") + timedelta(days=1)
    else:
        start_date = START_DATE
else:
    start_date = START_DATE

print(f"🚀 Bắt đầu crawl từ {start_date.strftime('%Y-%m-%d')} đến {END_DATE.strftime('%Y-%m-%d')}")

# 🌀 Lặp qua từng ngày mới
for i in tqdm(range((END_DATE - start_date).days + 1)):
    date = (start_date + timedelta(days=i)).strftime("%Y%m%d")
    url = f"https://edge-api.pnj.io/ecom-frontend/v1/get-gold-price-history?date={date}"
    
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            all_data.append({"date": date, "data": data})
        else:
            print(f"⚠️ {date}: Status code {r.status_code}")
    except Exception as e:
        print(f"❌ Lỗi ngày {date}: {e}")

# 💾 Lưu toàn bộ dữ liệu
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(all_data, f, ensure_ascii=False, indent=2)

# 🕓 Cập nhật metadata
meta = {"last_crawled_date": END_DATE.strftime("%Y%m%d")}
with open(META_FILE, "w", encoding="utf-8") as f:
    json.dump(meta, f, ensure_ascii=False, indent=2)

print(f"✅ Crawl hoàn tất, dữ liệu lưu tại: {OUTPUT_FILE}")
print(f"🕓 Ngày crawl mới nhất: {END_DATE.strftime('%Y-%m-%d')}")
