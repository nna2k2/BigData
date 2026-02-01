# Có cần cài Spark không?

## ❌ KHÔNG CẦN cài Spark riêng!

### Lý do:

1. **PySpark tự động tải Spark**: Khi cài PySpark qua pip, nó sẽ tự động download Spark binaries khi chạy lần đầu.

2. **Local mode**: Code đã được cấu hình để chạy ở `local[*]` mode, không cần Spark cluster.

3. **Chỉ cần cài PySpark**: 

```bash
pip3 install pyspark
```

### Khi nào CẦN cài Spark riêng?

Chỉ cần cài Spark riêng nếu:
- Chạy trên Spark cluster (YARN, Mesos, Kubernetes)
- Cần Spark standalone cluster
- Cần các tính năng nâng cao của Spark

### Với dự án này:

**KHÔNG CẦN** cài Spark riêng, chỉ cần:

```bash
# Cài PySpark và dependencies
pip3 install pyspark pandas numpy scikit-learn fuzzywuzzy python-Levenshtein

# Chạy job
python3 daily_gold_job_normalization_spark.py
```

PySpark sẽ tự động:
- Tải Spark binaries lần đầu chạy (có thể mất vài phút)
- Lưu vào cache để lần sau chạy nhanh hơn
- Chạy ở local mode trên máy hiện tại

### Kiểm tra:

```bash
# Kiểm tra PySpark đã cài chưa
python3 -c "import pyspark; print(pyspark.__version__)"

# Nếu chưa cài:
pip3 install pyspark
```

### Lưu ý:

- Lần đầu chạy có thể mất vài phút để PySpark tải Spark binaries
- Spark sẽ được cache trong `~/.cache/pyspark/` hoặc tương tự
- Không cần cấu hình gì thêm, PySpark tự động xử lý

