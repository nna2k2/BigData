-- Script SQL để tạo bảng ETL_CHECKPOINT
-- Chạy trong Oracle SQL Developer hoặc sqlplus

CREATE TABLE CLOUD.ETL_CHECKPOINT (
    JOB_NAME VARCHAR2(100) PRIMARY KEY,
    LAST_RUN TIMESTAMP
);

-- Kiểm tra bảng đã được tạo
SELECT * FROM CLOUD.ETL_CHECKPOINT;

