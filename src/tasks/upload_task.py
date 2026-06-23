"""
upload_task.py - Task 1: Upload Excel lên MinIO và ghi FileRegistry.

Trách nhiệm:
  1. Tính MD5 của file local
  2. So sánh với bản ghi gần nhất trong FileRegistry
     - Nếu MD5 giống → skip (trả về batch_id cũ để Task 2 vẫn chạy được nếu cần)
     - Nếu MD5 khác hoặc chưa có → upload lên MinIO, ghi FileRegistry
  3. Trả về UploadResult để Task 2 / Airflow XCom nhận

Không đọc Excel, không chạm PostgreSQL staging.
Airflow: PythonOperator gọi hàm `run()`, push result vào XCom.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from src.config_loader import ExcelFileConfig, PipelineConfig
from src.file_registry import FileRegistry
from src.minio_client import MinioClient

logger = logging.getLogger(__name__)


@dataclass
class UploadResult:
    file_id: str
    batch_id: str
    minio_path: str
    md5: str
    skipped: bool       # True = file không đổi, bỏ qua upload
    file_date: date


def run(
    file_path: str,
    file_id: str,
    config: PipelineConfig,
    batch_id: str,
    force_upload: bool = False,
) -> UploadResult:
    """
    Upload một file Excel lên MinIO nếu MD5 thay đổi.

    Args:
        file_path:    Đường dẫn file Excel local
        file_id:      ID trong config YAML
        config:       PipelineConfig đã parse
        batch_id:     batch_id duy nhất do orchestrator tạo
        force_upload: Bỏ qua MD5 check, luôn upload

    Returns:
        UploadResult chứa minio_path và batch_id để Task 2 dùng.
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

    local_md5 = _md5_file(file_path)
    today = date.today()

    # Kiểm tra MD5
    if not force_upload:
        latest = registry.get_latest(file_id)
        if latest and latest["md5"] == local_md5:
            logger.info(
                f"[upload_task] {file_id}: MD5 không đổi ({local_md5[:8]}…), bỏ qua upload."
            )
            return UploadResult(
                file_id=file_id,
                batch_id=latest["batch_id"],
                minio_path=latest["minio_path"],
                md5=local_md5,
                skipped=True,
                file_date=latest["file_date"],
            )

    # Upload lên MinIO
    logger.info(f"[upload_task] {file_id}: Uploading → MinIO (batch={batch_id})")
    minio_path = minio.upload_raw_excel(
        local_path=file_path,
        minio_prefix=file_cfg.minio_prefix,
        file_id=file_id,
        batch_id=batch_id,
    )

    # Ghi FileRegistry
    registry.insert(
        file_id=file_id,
        batch_id=batch_id,
        minio_path=minio_path,
        md5=local_md5,
        file_date=today,
        status="uploaded",
    )
    logger.info(f"[upload_task] {file_id}: ✓ {minio_path}")

    return UploadResult(
        file_id=file_id,
        batch_id=batch_id,
        minio_path=minio_path,
        md5=local_md5,
        skipped=False,
        file_date=today,
    )


def _md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()