"""
excel_pipeline_dag.py - DAG chính: Nạp tự động (Auto) hoặc thủ công (Force Run).

FIXES so với phiên bản cũ:
  Fix 1 — Import SDK Airflow 3.x (dag, task, Param từ airflow.sdk)
  Fix 2 — expand() trên list rỗng gây lỗi "no task to map" khi priority=[];
           dùng task chained thay vì expand trực tiếp trên list rỗng.
  Fix 3 — DAG run conf là dict, không dùng được context["dag_run"].conf khi
           chạy từ Scheduler (conf=None); đã thêm guard `or {}`.
  Fix 4 — Thiếu dependency giữa loaded_priority và uploaded_others khi dùng
           dynamic task mapping — dùng trigger_rule để tránh skip cascade.
"""
from __future__ import annotations
import logging
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# ─── Fix 1: Import từ airflow.sdk (Airflow 3.x) ───────────────────────────
try:
    from airflow.sdk import dag, task, Param  # Airflow 3.x
except ImportError:
    from airflow.decorators import dag, task   # Airflow 2.x fallback
    from airflow.models.param import Param
# ──────────────────────────────────────────────────────────────────────────

from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = "/mnt/c/excel-pipeline"
CONFIG_PATH  = f"{PROJECT_ROOT}/config/pipeline_config.yaml"
LANDING_DIR  = "data/raw"

# File_id nào cần chạy trước các file khác (Dim trước Fact)
PRIORITY_FILE_IDS = {"dim_bank"}

logger = logging.getLogger(__name__)


# =============================================================================
# UI PARAMS
# =============================================================================
AUTO_CHOICE = "--- Tự động quét tất cả file thay đổi (Auto) ---"

UI_PARAMS = {
    "report_name": Param(
        AUTO_CHOICE,
        type="string",
        enum=[
            AUTO_CHOICE,
            "1. Báo cáo Tín dụng & TSĐB (bc_tin_dung_2026.xlsx + Hop_dong_tien_gui.xlsm)",
            "2. Báo cáo Tài chính B01 (B01 DN Bao cao tinh hinh tai chinh.xlsx)",
            "3. Báo cáo KQKD B02 (B02 DN Bao cao ket qua hoat dong kinh doanh.xlsx)",
            "4. Công nợ Phải Thu KH (Chi_tiet_cong_no_phai_thu_khach_hang.xlsx)",
            "5. Công nợ Phải Trả NCC (Chi_tiet_cong_no_phai_tra_nha_cung_cap.xlsx)",
            "6. Sổ chi tiết Dòng tiền (So_chi_tiet_cac_tai_khoan.xlsx)",
            "7. Tổng hợp Tồn kho (Tong_hop_ton_kho.xlsx)",
            "8. Sổ chi tiết Mua hàng (So_chi_tiet_mua_hang.xlsx)",
            "9. Sổ chi tiết Bán hàng (So_chi_tiet_ban_hang.xlsx)",
            "10. Kế hoạch Kinh doanh (Ke_hoach_kinh_doanh_minh_long_2026.xlsx)",
            "11. Danh mục Đối tác (Danh_sach_khach_hang.xlsx + Danh_sach_nha_cung_cap.xlsx)",
            "12. Danh mục TK & Ngân hàng (Danh_sach_ngan_hang.xlsx + he_thong_tai_khoan + tai_khoan_ngan_hang)",
            "13. Danh mục Hàng hóa & Kho (Danh_sach_hang_hoa_dich_vu.xlsx + Danh_sach_kho.xlsx)",
        ],
        description=(
            "Chọn báo cáo muốn NẠP ÉP (Force Run). "
            "Để 'Auto' hệ thống tự quét file thay đổi (so MD5)."
        ),
    )
}

# Map label UI → danh sách file_id
_REPORT_MAP: dict[str, list[str]] = {
    AUTO_CHOICE: [],
    "1. Báo cáo Tín dụng & TSĐB (bc_tin_dung_2026.xlsx + Hop_dong_tien_gui.xlsm)":
        ["fact_loan", "fact_collateral", "fact_credit_limit_summary", "fact_term_deposit"],
    "2. Báo cáo Tài chính B01 (B01 DN Bao cao tinh hinh tai chinh.xlsx)":
        ["fact_balance_sheet", "dim_report_item_b01"],
    "3. Báo cáo KQKD B02 (B02 DN Bao cao ket qua hoat dong kinh doanh.xlsx)":
        ["fact_income_statement", "dim_report_item_b02"],
    "4. Công nợ Phải Thu KH (Chi_tiet_cong_no_phai_thu_khach_hang.xlsx)":
        ["fact_accounts_receivable"],
    "5. Công nợ Phải Trả NCC (Chi_tiet_cong_no_phai_tra_nha_cung_cap.xlsx)":
        ["fact_accounts_payable"],
    "6. Sổ chi tiết Dòng tiền (So_chi_tiet_cac_tai_khoan.xlsx)":
        ["fact_cashflow"],
    "7. Tổng hợp Tồn kho (Tong_hop_ton_kho.xlsx)":
        ["fact_inventory_balance"],
    "8. Sổ chi tiết Mua hàng (So_chi_tiet_mua_hang.xlsx)":
        ["fact_inventory_inward"],
    "9. Sổ chi tiết Bán hàng (So_chi_tiet_ban_hang.xlsx)":
        ["fact_inventory_outward"],
    "10. Kế hoạch Kinh doanh (Ke_hoach_kinh_doanh_minh_long_2026.xlsx)":
        ["fact_business_plan"],
    "11. Danh mục Đối tác (Danh_sach_khach_hang.xlsx + Danh_sach_nha_cung_cap.xlsx)":
        ["dim_partner_khach_hang", "dim_partner_nha_cung_cap"],
    "12. Danh mục TK & Ngân hàng (Danh_sach_ngan_hang.xlsx + he_thong_tai_khoan + tai_khoan_ngan_hang)":
        ["dim_account", "dim_account_number", "dim_bank"],
    "13. Danh mục Hàng hóa & Kho (Danh_sach_hang_hoa_dich_vu.xlsx + Danh_sach_kho.xlsx)":
        ["dim_product", "dim_warehouse"],
}


# =============================================================================
# Helpers
# =============================================================================

def _project_imports():
    import sys
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    from src.config_loader import load_config
    from src.file_matcher import match_files_in_dir
    from src.tasks import load_task, upload_task
    return load_config, match_files_in_dir, load_task, upload_task


def _is_force_run(params: dict, conf: dict) -> bool:
    """True khi chạy thủ công qua UI hoặc conf có force_upload=True."""
    report_name = params.get("report_name", AUTO_CHOICE)
    return report_name != AUTO_CHOICE or bool(conf.get("force_upload", False))


# =============================================================================
# DAG
# =============================================================================

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="excel_pipeline",
    default_args=default_args,
    description="Nạp dữ liệu Excel: Tự động (Auto) hoặc Thủ công (Force Run qua UI)",
    schedule="0 0 * * 5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params=UI_PARAMS,
    tags=["excel", "pipeline", "silver"],
)
def excel_pipeline_dag():

    # ─────────────────────────────────────────────────────────────────────
    # TASK 1: Quét file và phân loại priority / normal
    # ─────────────────────────────────────────────────────────────────────
    @task
    def detect_changed_files(**context) -> dict[str, list[dict[str, str]]]:
        import os
        os.chdir(PROJECT_ROOT)

        load_config, match_files_in_dir, _, _ = _project_imports()

        # Fix 3: conf có thể là None khi chạy từ scheduler
        params  = context.get("params") or {}
        dag_run = context.get("dag_run")
        conf    = (dag_run.conf if dag_run else None) or {}

        report_name  = params.get("report_name", AUTO_CHOICE)
        ui_file_ids  = set(_REPORT_MAP.get(report_name, []))
        extra_ids    = set(conf.get("file_ids") or [])
        only_file_ids = ui_file_ids | extra_ids  # rỗng = lấy tất cả

        config = load_config(CONFIG_PATH)

        # Quét đệ quy toàn bộ landing_dir
        all_matches: dict[str, list[str]] = {}
        for root, _dirs, _files in os.walk(LANDING_DIR):
            sub = match_files_in_dir(root, config)
            if sub:
                all_matches.update(sub)

        if not all_matches:
            logger.warning("Không tìm thấy file nào khớp trong %s", LANDING_DIR)

        priority, normal = [], []
        for file_path, file_ids in all_matches.items():
            for file_id in file_ids:
                if only_file_ids and file_id not in only_file_ids:
                    continue
                item = {"file_path": file_path, "file_id": file_id}
                (priority if file_id in PRIORITY_FILE_IDS else normal).append(item)

        logger.info(
            "Phát hiện %d task wave 1 (priority) + %d task wave 2 (normal)",
            len(priority), len(normal),
        )
        return {"priority": priority, "normal": normal}

    # ─────────────────────────────────────────────────────────────────────
    # Tách priority / normal từ XCom
    # ─────────────────────────────────────────────────────────────────────
    @task
    def extract_priority(files_dict: dict) -> list[dict]:
        return files_dict.get("priority") or []

    @task
    def extract_normal(files_dict: dict) -> list[dict]:
        return files_dict.get("normal") or []

    # ─────────────────────────────────────────────────────────────────────
    # TASK 2: Upload lên MinIO
    # Trả về upload_result dict để task load đọc.
    # Nếu skipped=True (MD5 không đổi) và không force → load sẽ tự bỏ qua.
    # ─────────────────────────────────────────────────────────────────────
    @task(retries=2, retry_delay=timedelta(minutes=2))
    def upload_one(file_info: dict[str, str], **context) -> dict[str, Any]:
        import os
        os.chdir(PROJECT_ROOT)
        load_config, _, _, upload_task = _project_imports()

        params  = context.get("params") or {}
        dag_run = context.get("dag_run")
        conf    = (dag_run.conf if dag_run else None) or {}
        force   = _is_force_run(params, conf)

        config   = load_config(CONFIG_PATH)
        batch_id = (
            f"{file_info['file_id']}"
            f"_{context['ts_nodash']}"
            f"_{uuid.uuid4().hex[:8]}"
        )

        result = upload_task.run(
            file_path=file_info["file_path"],
            file_id=file_info["file_id"],
            config=config,
            batch_id=batch_id,
            force_upload=force,
        )

        logger.info(
            "[upload] %s | batch=%s | skipped=%s | path=%s",
            result.file_id, result.batch_id, result.skipped, result.minio_path,
        )
        return {
            "file_id":    result.file_id,
            "batch_id":   result.batch_id,
            "minio_path": result.minio_path,
            "skipped":    result.skipped,
        }

    # ─────────────────────────────────────────────────────────────────────
    # TASK 3: Load vào PostgreSQL
    # Fix 4: trigger_rule=ALL_DONE agar task này không bị skip cascade
    #        khi upstream dynamic task có 1 instance fail.
    # ─────────────────────────────────────────────────────────────────────
    @task(
        retries=1,
        retry_delay=timedelta(minutes=3),
        trigger_rule=TriggerRule.ALL_DONE,   # Fix 4
    )
    def load_one(upload_result: dict[str, Any], **context) -> dict[str, Any]:
        import os
        os.chdir(PROJECT_ROOT)
        load_config, _, load_task, _ = _project_imports()

        params  = context.get("params") or {}
        dag_run = context.get("dag_run")
        conf    = (dag_run.conf if dag_run else None) or {}
        force   = _is_force_run(params, conf) or bool(conf.get("force_load", False))

        # Skip nếu upload không đổi và không force
        if upload_result["skipped"] and not force:
            logger.info(
                "[load] %s: MD5 không đổi + force=False → bỏ qua.",
                upload_result["file_id"],
            )
            return {"file_id": upload_result["file_id"], "status": "skipped", "rows": 0}

        config = load_config(CONFIG_PATH)
        result = load_task.run(
            file_id=upload_result["file_id"],
            batch_id=upload_result["batch_id"],
            minio_path=upload_result["minio_path"],
            config=config,
        )

        if not result.success:
            failed = {
                s: r.error
                for s, r in result.sheets.items()
                if r.status == "error"
            }
            raise RuntimeError(
                f"Load '{upload_result['file_id']}' thất bại: {failed}"
            )

        logger.info(
            "[load] %s: %d dòng | batch=%s",
            upload_result["file_id"], result.total_rows, upload_result["batch_id"],
        )
        return {
            "file_id": upload_result["file_id"],
            "status":  "success",
            "rows":    result.total_rows,
        }

    # ─────────────────────────────────────────────────────────────────────
    # TASK 4 (optional): Tổng kết và ghi log cuối DAG run
    # ─────────────────────────────────────────────────────────────────────
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize(
        priority_results: list[dict],
        normal_results: list[dict],
    ) -> dict[str, Any]:
        # Fix: ép về list thật trước khi concat
        p = list(priority_results) if priority_results else []
        n = list(normal_results)   if normal_results   else []
        all_results = p + n

        success    = [r for r in all_results if r.get("status") == "success"]
        skipped    = [r for r in all_results if r.get("status") == "skipped"]
        failed     = [r for r in all_results if r.get("status") not in ("success", "skipped")]
        total_rows = sum(r.get("rows", 0) for r in success)

        logger.info("=" * 60)
        logger.info("TỔNG KẾT DAG RUN")
        logger.info("  ✓ Thành công : %d file | %d dòng", len(success), total_rows)
        logger.info("  ⏭ Bỏ qua    : %d file (MD5 không đổi)", len(skipped))
        logger.info("  ✗ Lỗi       : %d file", len(failed))
        if failed:
            for r in failed:
                logger.error("    - %s", r.get("file_id", "unknown"))
        logger.info("=" * 60)

        return {
            "success_count": len(success),
            "skipped_count": len(skipped),
            "failed_count":  len(failed),
            "total_rows":    total_rows,
        }
    # ─────────────────────────────────────────────────────────────────────
    # WIRE UP — 2 wave, priority xong trước normal
    #
    #   detect → extract_priority → upload_one[] → load_one[]  ─┐
    #                                                             ├→ summarize
    #   detect → extract_normal   → upload_one[] → load_one[]  ─┘
    #
    # loaded_priority >> uploaded_normal đảm bảo Dim load xong trước Fact
    # ─────────────────────────────────────────────────────────────────────
   # ─── WIRE UP ───────────────────────────────────────────────────────────────
    files_dict = detect_changed_files()

    priority_list = extract_priority(files_dict)
    normal_list   = extract_normal(files_dict)

    # Wave 1: Priority (Dim)
    uploaded_priority = upload_one.expand(file_info=priority_list)
    loaded_priority   = load_one.expand(upload_result=uploaded_priority)

    # Wave 2: Normal (Fact) — thêm .override() để không bị skip cascade từ wave 1
    uploaded_normal = upload_one.override(
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(file_info=normal_list)

    loaded_normal = load_one.override(
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(upload_result=uploaded_normal)

    # Wave 1 xong mới chạy Wave 2
    loaded_priority >> uploaded_normal

    # Tổng kết
    summarize(
        priority_results=loaded_priority,
        normal_results=loaded_normal,
    )


excel_pipeline_dag()