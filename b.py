import json
import csv

# 🗂️ Đọc file JSON
with open("pnj_gold_price_history_2022_2025.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# 🧾 Chuẩn bị danh sách dòng dữ liệu CSV
rows = []

# Duyệt qua từng ngày
for day_entry in data:
    date = day_entry.get("date")
    locations = day_entry.get("data", {}).get("locations", [])
    
    for loc in locations:
        location_name = loc.get("name")
        for gold_type in loc.get("gold_type", []):
            gold_name = gold_type.get("name")
            for item in gold_type.get("data", []):
                rows.append({
                    "date": date,
                    "location": location_name,
                    "gold_type": gold_name,
                    "gia_mua": item.get("gia_mua"),
                    "gia_ban": item.get("gia_ban"),
                    "updated_at": item.get("updated_at"),
                })

# 💾 Ghi ra file CSV
output_file = "pnj_gold_price_history.csv"
with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"✅ Đã chuyển thành công sang CSV: {output_file}")
print(f"📊 Tổng số dòng: {len(rows)}")
