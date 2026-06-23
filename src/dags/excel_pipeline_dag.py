"""
excel_pipeline_dag.py - DAG chính: quét landing folder, tự map file -> (các) file_id
liên quan, rồi upload + load TỪNG file_id như 1 task riêng.

KHÁC VỚI BẢN CŨ:
  Bản cũ có 1 danh sách FILE_TASKS hard-code cứng (file_path_template, file_id),
  không tận dụng `source_pattern` đã khai báo sẵn trong pipeline_config.yaml.
  Mỗi khi có file mới phải tự sửa DAG.

  Bản mới: 1 task `detect_changed_files` quét LANDING_DIR, dùng
  `src/file_matcher.py` để so khớp tên file với source_pattern của TỪNG
  file_id trong config — 1 file có thể khớp NHIỀU file_id (vd file
  "bc_tin_dung*.xlsx" sinh ra cả fact_loan + fact_credit_limit_summary +
  fact_collateral). Mỗi (file, file_id) khớp được trở thành 1 task upload +
  1 task load riêng, chạy qua Airflow Dynamic Task Mapping (.expand()) —
  KHÔNG cần sửa DAG khi thêm file_id mới trong YAML.

  upload_task tự so MD5 với FileRegistry — file không đổi sẽ tự skip ở bước
  upload, và load_one() sẽ skip theo (trừ khi force_load=True trong dag conf).
  => Đúng yêu cầu "file nào đổi thì chỉ chạy lại file đó".

THỨ TỰ NẠP DIM_BANK TRƯỚC:
  Dim_AccountNumber cần tra Bank_Code từ Dim_Bank (xem
  src/transformers/dim_account_number.py). Vì vậy các file_id trong
  PRIORITY_FILE_IDS được upload+load thành 1 "wave" riêng, xong mới chạy
  wave còn lại.

Cấu hình qua dag_run.conf (trigger DAG w/ config):
  {
    "landing_dir": "/data/raw",       # mặc định LANDING_DIR
    "file_ids": ["fact_loan"],        # chỉ chạy các file_id này (rỗng = tất cả khớp được)
    "force_upload": false,            # bỏ qua MD5 check, luôn upload lại
    "force_load": false               # vẫn load dù upload bị skip (MD5 không đổi)
  }

Cài đặt:
  pip install apache-airflow
  export AIRFLOW_HOME=~/airflow
  airflow db init
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from airflow.decorators import dag, task

# Điều chỉnh đường dẫn tới project của bạn trên Airflow worker
PROJECT_ROOT = "/opt/excel-pipeline"
CONFIG_PATH = f"{PROJECT_ROOT}/config/pipeline_config.yaml"
LANDING_DIR = "/data/raw"  # quét đệ quy thư mục này (gồm cả raw/misa, raw/mailan...)

# file_id cần được load TRƯỚC các file_id khác (vì có transformer phụ thuộc lookup)
PRIORITY_FILE_IDS = ["dim_bank"]

logger = logging.getLogger(__name__)


def _project_imports():
    """Import muộn (sau khi sys.path đã chỉnh) để Airflow parse DAG file không lỗi
    khi project chưa nằm trong PYTHONPATH lúc DAG file được Airflow scan."""
    import sys
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    from src.config_loader import load_config
    from src.file_matcher import match_files_in_dir
    from src.tasks import load_task, upload_task
    return load_config, match_files_in_dir, load_task, upload_task


default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": __import__("datetime").timedelta(minutes=5),
}


@dag(
    dag_id="excel_pipeline",
    default_args=default_args,
    description="Excel → MinIO → Silver: tự phát hiện file đổi, tự map sang bảng liên quan",
    schedule="0 7 * * *",      # Airflow >= 2.4 dùng `schedule`; bản cũ dùng `schedule_interval`
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["excel", "pipeline", "silver"],
)
def excel_pipeline_dag():

    @task
    def detect_changed_files(**context) -> Dict[str, List[Dict[str, str]]]:
        """
        Quét LANDING_DIR, so khớp source_pattern -> ra danh sách (file_path, file_id).
        Tách riêng các file_id nằm trong PRIORITY_FILE_IDS thành wave 1.
        """
        load_config, match_files_in_dir, _, _ = _project_imports()

        conf = (context.get("dag_run").conf if context.get("dag_run") else {}) or {}
        landing_dir = conf.get("landing_dir", LANDING_DIR)
        only_file_ids = set(conf.get("file_ids") or [])

        config = load_config(CONFIG_PATH)
        matches = match_files_in_dir(landing_dir, config)  # {file_path: [file_id, ...]}

        if not matches:
            logger.warning(f"Không tìm thấy file nào khớp source_pattern trong {landing_dir}")

        priority, normal = [], []
        for file_path, file_ids in matches.items():
            for file_id in file_ids:
                if only_file_ids and file_id not in only_file_ids:
                    continue
                item = {"file_path": file_path, "file_id": file_id}
                if file_id in PRIORITY_FILE_IDS:
                    priority.append(item)
                else:
                    normal.append(item)

        logger.info(
            f"Phát hiện {len(priority)} task wave 1 (priority) + "
            f"{len(normal)} task wave 2 (normal)"
        )
        return {"priority": priority, "normal": normal}

    @task
    def upload_one(file_info: Dict[str, str], **context) -> Dict[str, Any]:
        load_config, _, _, upload_task = _project_imports()
        conf = (context.get("dag_run").conf if context.get("dag_run") else {}) or {}
        force_upload = bool(conf.get("force_upload", False))

        config = load_config(CONFIG_PATH)
        batch_id = f"{file_info['file_id']}_{context['ts_nodash']}_{uuid.uuid4().hex[:8]}"

        result = upload_task.run(
            file_path=file_info["file_path"],
            file_id=file_info["file_id"],
            config=config,
            batch_id=batch_id,
            force_upload=force_upload,
        )
        return {
            "file_id": result.file_id,
            "batch_id": result.batch_id,
            "minio_path": result.minio_path,
            "skipped": result.skipped,
        }

    @task
    def load_one(upload_result: Dict[str, Any], **context) -> Dict[str, Any]:
        load_config, _, load_task, _ = _project_imports()
        conf = (context.get("dag_run").conf if context.get("dag_run") else {}) or {}
        force_load = bool(conf.get("force_load", False))

        if upload_result["skipped"] and not force_load:
            logger.info(
                f"[load] {upload_result['file_id']}: upload skipped (MD5 không đổi) "
                f"+ force_load=False -> bỏ qua load."
            )
            return {"file_id": upload_result["file_id"], "status": "skipped"}

        config = load_config(CONFIG_PATH)
        result = load_task.run(
            file_id=upload_result["file_id"],
            batch_id=upload_result["batch_id"],
            minio_path=upload_result["minio_path"],
            config=config,
        )

        if not result.success:
            failed = {s: r.error for s, r in result.sheets.items() if r.status == "error"}
            raise RuntimeError(f"Load {upload_result['file_id']} thất bại: {failed}")

        return {"file_id": upload_result["file_id"], "status": "success", "rows": result.total_rows}

    files = detect_changed_files()

    # Wave 1: các file_id mà bảng khác phụ thuộc lookup (vd dim_bank) -> load xong hết mới qua wave 2
    uploaded_priority = upload_one.expand(file_info=files["priority"])
    loaded_priority = load_one.expand(upload_result=uploaded_priority)

    # Wave 2: tất cả file_id còn lại
    uploaded_normal = upload_one.expand(file_info=files["normal"])
    loaded_normal = load_one.expand(upload_result=uploaded_normal)

    loaded_priority >> uploaded_normal


excel_pipeline_dag()
