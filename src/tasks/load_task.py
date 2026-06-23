"""
load_task.py - Task 2: Đọc file từ MinIO và load vào PostgreSQL staging.

Nhận minio_path + batch_id từ UploadResult (XCom hoặc truyền trực tiếp).
Có thể chạy độc lập với Task 1 — chỉ cần biết minio_path.

Trách nhiệm:
  1. Download file Excel từ MinIO về /tmp
  2. Validate cột Excel theo config
  3. Tạo / sync staging table nếu chưa có
  4. Đọc từng sheet, apply transformer, load vào DB
  5. Cập nhật FileRegistry status
  6. Ghi _pipeline_log

Airflow: PythonOperator gọi `run()`, nhận minio_path từ XCom của upload_task.
"""
from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

from src.config_loader import ExcelFileConfig, PipelineConfig
from src.file_registry import FileRegistry
from src.loaders.pg_staging import PgStagingManager
from src.minio_client import MinioClient
from src.readers.excel_reader import ExcelReader, validate_excel_columns

logger = logging.getLogger(__name__)


@dataclass
class SheetResult:
    status: str                     # "success" | "error"
    rows: int = 0
    error: Optional[str] = None


@dataclass
class LoadResult:
    file_id: str
    batch_id: str
    sheets: Dict[str, SheetResult] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return all(r.status == "success" for r in self.sheets.values())

    @property
    def total_rows(self) -> int:
        return sum(r.rows for r in self.sheets.values())


def run(
    file_id: str,
    batch_id: str,
    minio_path: str,
    config: PipelineConfig,
    skip_if_unchanged: bool = True,
) -> LoadResult:
    """
    Load một file_id từ MinIO vào PostgreSQL staging.

    Args:
        file_id:            ID trong config YAML
        batch_id:           batch_id từ upload_task (để ghi log)
        minio_path:         Đường dẫn MinIO đầy đủ (s3://bucket/...)
        config:             PipelineConfig đã parse
        skip_if_unchanged:  Nếu True và registry cho thấy batch này đã loaded → skip

    Returns:
        LoadResult với kết quả từng sheet.
    """
    file_cfg: ExcelFileConfig = config.get_file_config(file_id)
    if not file_cfg:
        raise ValueError(f"Không tìm thấy file_id='{file_id}'")

    pg_cfg = config.connections.postgresql
    registry = FileRegistry(
        config=pg_cfg,
        schema=pg_cfg.get("schema_staging", "bronze"),
    )
    minio = MinioClient(config.connections.minio)
    pg = PgStagingManager(
        config=pg_cfg,
        schema=pg_cfg.get("schema_staging", "bronze"),
    )

    result = LoadResult(file_id=file_id, batch_id=batch_id)

    # Download file từ MinIO về /tmp
    suffix = Path(minio_path).suffix or ".xlsx"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        local_path = tmp.name

    logger.info(f"[load_task] {file_id}: Downloading {minio_path} → {local_path}")
    minio.download_for_processing(minio_path, local_path)

    try:
        # Validate cột
        logger.info(f"[load_task] {file_id}: Validating columns…")
        issues = validate_excel_columns(local_path, file_cfg)
        if issues:
            msg = "; ".join(f"sheet '{s}': thiếu {cols}" for s, cols in issues.items())
            raise ValueError(f"Lỗi mapping cột: {msg}")

        # Tạo / sync staging tables
        for sheet_cfg in file_cfg.sheets:
            pg.create_or_update_table(sheet_cfg)

        # Đọc và load từng sheet
        # (pg=pg để các transformer cần lookup bảng đã load, vd Dim_Bank, dùng được)
        reader = ExcelReader(file_cfg, null_values=config.defaults.get("null_values"), pg=pg)

        for sheet_cfg, df in reader.read_all_sheets(local_path):
            sheet_label = sheet_cfg.sheet_name or f"index_{sheet_cfg.sheet_index}"
            try:
                rows = pg.load_dataframe(
                    df=df,
                    sheet_cfg=sheet_cfg,
                    batch_id=batch_id,
                    source_file=minio_path,
                )
                result.sheets[sheet_label] = SheetResult(status="success", rows=rows)
                pg.log_pipeline_run(
                    batch_id=batch_id, file_id=file_id,
                    sheet_name=sheet_label, target_table=sheet_cfg.target_table,
                    rows_loaded=rows, status="success", source_file=minio_path,
                )
                logger.info(f"[load_task] {file_id}/{sheet_label}: ✓ {rows:,} dòng")

            except Exception as exc:
                logger.error(f"[load_task] {file_id}/{sheet_label}: ✗ {exc}")
                result.sheets[sheet_label] = SheetResult(status="error", error=str(exc))
                pg.log_pipeline_run(
                    batch_id=batch_id, file_id=file_id,
                    sheet_name=sheet_label, target_table=sheet_cfg.target_table,
                    rows_loaded=0, status="error", error_msg=str(exc),
                    source_file=minio_path,
                )

        # Cập nhật registry status
        final_status = "loaded" if result.success else "failed"
        for sheet_label in result.sheets:
            registry.update_status(batch_id, sheet_label, final_status)

    finally:
        # Dọn file tạm
        Path(local_path).unlink(missing_ok=True)

    return result