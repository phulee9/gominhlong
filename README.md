# Excel → MinIO → Silver Pipeline

Luồng ingestion từ file Excel vào lớp Silver (Dim/Fact), thiết kế để chạy được
cả qua CLI (`main.py`) và Airflow (`src/dags/`), có rollback theo ngày, và tự
phát hiện file Excel nào cần nạp lại khi nó thay đổi.

---

## Kiến trúc tổng thể

```
                pipeline_config.yaml
        (source_pattern, field_mapping, transformer_class)
                         │
                         ▼
   ┌─────────────┐   Task 1: UPLOAD    ┌─────────┐
   │ Excel File  │ ──────────────────► │  MinIO  │  raw-excel/ (lưu nguyên bản)
   └─────────────┘   (so MD5, skip      └─────────┘
                       nếu không đổi)        │
                                             │ FileRegistry ghi: file_id,
                                             │ batch_id, md5, file_date, status
                                             ▼
                                     ┌────────────────┐
                                     │ FileRegistry    │  (bronze._file_registry)
                                     │ (DB - Postgres) │
                                     └────────────────┘
                                             │
                         Task 2: LOAD        │ batch_id + minio_path
                         (độc lập Task 1,    │
                          nhận qua XCom)      ▼
   ┌─────────────┐   download    ┌──────────────────┐   transform   ┌─────────┐
   │   MinIO     │ ────────────► │   ExcelReader     │ ────────────► │ Silver  │
   │ raw-excel/  │               │ (đọc + field_map) │  (per-table   │ Dim/Fact│
   └─────────────┘               └──────────────────┘   transformer)└─────────┘
```

**2 task tách biệt, đúng kiểu Airflow**: Task 1 (upload) và Task 2 (load) là 2
hàm độc lập (`src/tasks/upload_task.py`, `src/tasks/load_task.py`), không phụ
thuộc lẫn nhau về runtime — Task 2 chỉ cần `batch_id` + `minio_path` (qua XCom),
không cần file Excel gốc còn trên máy.

---

## Cấu trúc dự án

```
excel-pipeline/
├── config/
│   └── pipeline_config.yaml     ← FILE CHÍNH: khai báo file_id, cột, transformer
├── src/
│   ├── config_loader.py          ← Parse YAML
│   ├── file_registry.py          ← Sổ cái upload/load (MD5, batch_id, rollback)
│   ├── file_matcher.py           ← Map filename -> (các) file_id qua source_pattern
│   ├── minio_client.py           ← Upload/download MinIO
│   ├── pipeline.py               ← Orchestrator (dùng cho CLI)
│   ├── readers/
│   │   └── excel_reader.py       ← Đọc Excel + field_mapping + cast dtype (GENERIC)
│   ├── transformers/              ← Business logic RIÊNG từng bảng (1 file/bảng)
│   │   ├── base.py                ← Interface chung (BaseTransformer, TransformContext)
│   │   ├── dim_partner.py
│   │   ├── dim_account_number.py  ← tra Bank_Code từ Dim_Bank (DB), KHÔNG đọc file sidecar
│   │   └── ... (18 transformer, 1 cho mỗi bảng có logic đặc biệt)
│   ├── loaders/
│   │   └── pg_staging.py         ← DDL + load + fetch_table() (lookup cho transformer)
│   ├── tasks/
│   │   ├── upload_task.py        ← Task 1: upload MinIO + ghi FileRegistry
│   │   ├── load_task.py          ← Task 2: download + transform + load DB
│   │   └── rollback_task.py      ← Rollback 1/nhiều file_id về batch cũ theo ngày
│   └── dags/
│       ├── excel_pipeline_dag.py   ← DAG chính: tự quét + map file -> file_id
│       └── excel_rollback_dag.py   ← DAG rollback (trigger thủ công)
├── main.py                        ← CLI entrypoint
├── docker-compose.yml             ← PostgreSQL + MinIO local (dev)
├── requirements.txt                ← Dependencies cho code trong src/ (không có Airflow)
└── requirements-airflow.txt        ← Thêm Airflow, cài trên máy chạy scheduler/worker
```

---

## Cài đặt (dev local)

```bash
pip install -r requirements.txt

docker compose up -d
# MinIO Console: http://localhost:9001  (minioadmin/minioadmin)
# PostgreSQL:    localhost:5432         (dev/Inda1234, db=data_warehouse)
```

---

## Dùng CLI

### 1. Không cần nhớ `--id` — CLI tự suy ra từ tên file

```bash
# Quét 1 thư mục, xem file nào khớp file_id nào (dùng source_pattern trong config)
python main.py detect --dir /data/raw/misa

# Chạy luôn không cần --id — tự suy ra qua source_pattern
# (nếu 1 file khớp NHIỀU file_id, CLI tự chạy lần lượt tất cả)

python main.py run --file /data/raw/mailan/bc_tin_dung_2026.xlsx
python main.py run --file data/raw/misa/B01_DN_Bao_cao_tinh_hinh_tai_chinh.xlsx data/raw/misa/B02_DN_Bao_cao_ket_qua_hoat_dong_kinh_doanh.xlsx
# Vẫn dùng được --id nếu muốn chỉ định chính xác
python main.py run --file bao_cao.xlsx --id fact_cashflow
```

### 2. Tách 2 bước (giống cách Airflow sẽ gọi)

```bash
python main.py upload --file bao_cao.xlsx --id fact_cashflow
python main.py load --id fact_cashflow --batch-id fact_cashflow_20260601_123456_abc \
    --minio-path s3://raw-excel/...
```

### 3. Rollback

```bash
# Rollback 1 file về batch gần nhất <= ngày chỉ định
python main.py rollback --date 2026-05-30 --ids fact_loan

# Rollback NHIỀU file
python main.py rollback --date 2026-05-30 --ids fact_loan,fact_cashflow

# Rollback TẤT CẢ file_id trong config
python main.py rollback --date 2026-05-30
```

Cơ chế: với mỗi `file_id`, tìm batch gần nhất trong `FileRegistry` có
`file_date <= target_date` (bỏ qua batch lỗi) → xoá dữ liệu hiện tại trong
bảng đích → load lại từ MinIO bằng batch cũ đó. Không cần file Excel gốc còn
trên máy vì bản đã lưu sẵn trên MinIO từ lúc upload.

### 4. Khác

```bash
python main.py setup-tables          # tạo tất cả bảng từ config
python main.py preview-ddl --id fact_loan
python main.py validate --file bao_cao.xlsx     # tự suy ra --id
python main.py history --id fact_cashflow       # xem lịch sử upload/load
```

---

## Chạy qua Airflow

```bash
pip install -r requirements-airflow.txt
export AIRFLOW_HOME=~/airflow
airflow db init
```

Copy project vào `/opt/excel-pipeline` trên Airflow host (hoặc sửa
`PROJECT_ROOT` trong 2 file DAG), rồi symlink/copy `src/dags/*.py` vào
`$AIRFLOW_HOME/dags/`.

- **`excel_pipeline` (chạy theo lịch, 7h sáng mỗi ngày)**: quét `LANDING_DIR`,
  dùng `file_matcher.py` để tự map file → (các) `file_id` liên quan, rồi
  upload + load từng file_id qua Dynamic Task Mapping. File không đổi (MD5
  giống lần trước) sẽ tự skip ở bước upload, và load cũng skip theo — chỉ
  bảng nào có file vừa đổi mới chạy lại.

  Trigger với config tuỳ chọn:
  ```json
  {"landing_dir": "/data/raw", "file_ids": ["fact_loan"], "force_upload": false, "force_load": false}
  ```

- **`excel_pipeline_rollback` (chỉ trigger thủ công)**:
  ```json
  {"target_date": "2026-05-30", "file_ids": ["fact_loan"]}
  ```
  `file_ids` rỗng = rollback tất cả.

---

## Thêm 1 file Excel / bảng mới

1. Thêm entry vào `excel_files` trong `pipeline_config.yaml` (file_id,
   source_pattern, field_mapping...).
2. Nếu bảng có logic riêng (lọc rác, tách cột, unpivot...): tạo
   `src/transformers/<ten_bang>.py` kế thừa `BaseTransformer`, đăng ký trong
   `src/transformers/__init__.py`, rồi khai báo `transformer_class: "<ten_bang>"`
   trong YAML. Không có logic riêng thì bỏ qua bước này.
3. `python main.py preview-ddl --id <file_id>` để xem DDL trước khi chạy thật.

### Khi transformer cần tra cứu 1 bảng khác đã load (vd Dim_Bank)

Dùng `ctx.lookup("Ten_Bang")` trong `transform()` — trả về `DataFrame` chứa
toàn bộ bảng đó từ DB (không đọc file Excel sidecar, tránh phụ thuộc layout
thư mục lúc chạy qua MinIO/Airflow). Xem ví dụ:
`src/transformers/dim_account_number.py`.

Nếu bảng A cần dữ liệu của bảng B, đảm bảo B được load TRƯỚC A:
- CLI: chạy `python main.py run --id <file_id_cua_B>` trước.
- Airflow: thêm `file_id` của B vào `PRIORITY_FILE_IDS` trong
  `src/dags/excel_pipeline_dag.py` (đã có sẵn `dim_bank`).

---

## Dtype hỗ trợ trong `field_mapping`

| Config dtype | PostgreSQL type |
|---|---|
| `varchar(N)` | `VARCHAR(N)` |
| `text` | `TEXT` |
| `integer` | `INTEGER` |
| `bigint` | `BIGINT` |
| `numeric(18,2)` | `NUMERIC(18,2)` |
| `date` | `DATE` |
| `timestamp` | `TIMESTAMP` |
| `boolean` | `BOOLEAN` |

## Load Mode (trên mỗi `sheet`)

| Config | Hành vi |
|--------|---------|
| `truncate_before_load: true` | Xóa toàn bộ data cũ, insert mới |
| `truncate_before_load: false` + `upsert_key: [...]` | Update nếu key tồn tại, insert nếu chưa |
| `truncate_before_load: false` + `delete_condition: "..."` | Xóa theo điều kiện rồi insert (delete-insert theo kỳ) |
| Còn lại | Append thuần |
