# Bảo vệ bảng gốc - Chỉ tạo bảng _CLEAN

## ✅ Đảm bảo không chỉnh bảng gốc

Code đã được thiết kế để **CHỈ TẠO BẢNG MỚI** với suffix `_CLEAN`, **KHÔNG BAO GIỜ** chỉnh sửa bảng gốc.

### Các bảng gốc (CHỈ ĐỌC):
- `LOCATION_DIMENSION` ✅ Chỉ đọc
- `GOLD_TYPE_DIMENSION` ✅ Chỉ đọc  
- `GOLD_PRICE_FACT` ✅ Chỉ đọc

### Các bảng mới được tạo (CHỈ GHI):
- `LOCATION_DIMENSION_CLEAN` ✅ Chỉ ghi
- `GOLD_TYPE_DIMENSION_CLEAN` ✅ Chỉ ghi
- `GOLD_PRICE_FACT_CLEAN` ✅ Chỉ ghi

## Kiểm tra code

Tất cả các hàm `write_table_to_oracle()` chỉ ghi vào bảng `_CLEAN`:

```python
# ✅ ĐÚNG - Ghi vào bảng CLEAN
write_table_to_oracle(df_final, f"{DB_USER}.LOCATION_DIMENSION_CLEAN", "overwrite")
write_table_to_oracle(df_enriched, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
write_table_to_oracle(df_fact_all, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite")

# ❌ KHÔNG CÓ - Không có dòng nào ghi vào bảng gốc
# write_table_to_oracle(..., "LOCATION_DIMENSION", ...)  # KHÔNG TỒN TẠI
# write_table_to_oracle(..., "GOLD_TYPE_DIMENSION", ...)  # KHÔNG TỒN TẠI
# write_table_to_oracle(..., "GOLD_PRICE_FACT", ...)  # KHÔNG TỒN TẠI
```

## Quy trình xử lý

1. **Đọc** từ bảng gốc (chỉ đọc, không chỉnh)
2. **Xử lý** dữ liệu trong memory (Spark DataFrame)
3. **Ghi** vào bảng `_CLEAN` mới

## Lợi ích

- ✅ Bảng gốc luôn an toàn, không bị thay đổi
- ✅ Có thể so sánh dữ liệu gốc vs dữ liệu đã làm sạch
- ✅ Có thể rollback bằng cách xóa bảng `_CLEAN`
- ✅ Có thể chạy lại job nhiều lần mà không lo mất dữ liệu gốc

## Lưu ý

- Bảng `ETL_CHECKPOINT` là bảng phụ trợ, có thể được tạo/cập nhật (không phải bảng dữ liệu chính)
- Tất cả snapshots được lưu ra CSV, không ảnh hưởng database

