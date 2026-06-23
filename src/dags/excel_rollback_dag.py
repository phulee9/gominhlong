"""
excel_rollback_dag.py - DAG rollback, chạy thủ công (Trigger DAG w/ config).

Dùng khi cần: "rollback lại ngày cũ/tuần cũ — chỉ 1 file hoặc all".

Trigger trong Airflow UI / CLI với dag_run.conf:
  {
    "target_date": "2026-05-30",          # bắt buộc, định dạng YYYY-MM-DD
    "file_ids": ["fact_loan", "fact_cashflow"]   # bỏ trống hoặc [] = rollback TẤT CẢ file_id trong config
  }

CLI tương đương (không cần Airflow):
  python main.py rollback --date 2026-05-30 --ids fact_loan,fact_cashflow
  python main.py rollback --date 2026-05-30            # rollback tất cả

Cơ chế (xem src/tasks/rollback_task.py):
  1. Với mỗi file_id, tìm batch gần nhất trong FileRegistry có file_date <= target_date
     (bỏ qua batch status='failed')
  2. Xóa dữ liệu hiện tại trong bảng đích (truncate hoặc delete_condition theo config)
  3. Load lại từ MinIO bằng batch cũ đó — KHÔNG cần file Excel gốc còn trên máy,
     vì bản thân đã được lưu trên MinIO ngay từ lúc upload.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

PROJECT_ROOT = "/opt/excel-pipeline"
CONFIG_PATH = f"{PROJECT_ROOT}/config/pipeline_config.yaml"

default_args = {"owner": "data-team", "retries": 0}


@dag(
    dag_id="excel_pipeline_rollback",
    default_args=default_args,
    description="Rollback 1/nhiều file_id (hoặc tất cả) về batch cũ theo ngày — chỉ chạy thủ công",
    schedule=None,           # chỉ trigger thủ công, không chạy theo lịch
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["excel", "pipeline", "rollback"],
)
def excel_rollback_dag():

    @task
    def run_rollback(**context):
        import sys
        if PROJECT_ROOT not in sys.path:
            sys.path.insert(0, PROJECT_ROOT)
        from datetime import date as _date

        from src.config_loader import load_config
        from src.tasks import rollback_task

        conf = (context.get("dag_run").conf if context.get("dag_run") else {}) or {}
        target_date_str = conf.get("target_date")
        if not target_date_str:
            raise AirflowFailException(
                "Thiếu 'target_date' trong dag_run.conf. "
                'Ví dụ: {"target_date": "2026-05-30", "file_ids": ["fact_loan"]}'
            )
        target_date = _date.fromisoformat(target_date_str)
        file_ids = conf.get("file_ids") or []  # rỗng = rollback tất cả

        config = load_config(CONFIG_PATH)
        results = rollback_task.run_batch(file_ids, target_date, config)

        failed = {fid: r.error for fid, r in results.items() if r.status == "error"}
        if failed:
            raise AirflowFailException(f"Rollback lỗi: {failed}")

        return {
            fid: {"status": r.status, "rolled_back_to_date": str(r.rolled_back_to_date)}
            for fid, r in results.items()
        }

    run_rollback()


excel_rollback_dag()
