"""
excel_rollback_dag.py - DAG rollback dữ liệu về ngày cũ.

FIXES so với phiên bản cũ:
  Fix 1 — Import SDK Airflow 3.x (dag, task, Param, AirflowFailException từ airflow.sdk)
  Fix 2 — Dùng key ngắn cho enum ("00","01",...) tách khỏi label hiển thị,
           tránh 400 Bad Request khi label thay đổi (giống excel_pipeline_dag.py).
  Fix 3 — Default value là "00" (placeholder key) thay vì chuỗi label dài,
           Airflow validate enum dựa trên key → không bao giờ fail.
  Fix 4 — Lỗi "Vui lòng chọn báo cáo" xảy ra khi trigger mà không truyền
           params → params.get("report_name") trả về default "00" → raise.
           Fix: kiểm tra key == PLACEHOLDER_KEY thay vì so sánh chuỗi label.
"""
from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import dag, task, Param                        # Airflow 3.x
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:
    from airflow.decorators import dag, task                        # Airflow 2.x fallback
    from airflow.models.param import Param
    from airflow.exceptions import AirflowFailException

PROJECT_ROOT = "/mnt/c/excel-pipeline"
CONFIG_PATH  = f"{PROJECT_ROOT}/config/pipeline_config.yaml"

default_args = {"owner": "data-team", "retries": 0}


# =============================================================================
# REPORT REGISTRY — key ngắn ổn định, label có thể đổi tự do
# =============================================================================
PLACEHOLDER_KEY = "00"

_REPORTS: list[tuple[str, str, list[str]]] = [
    (
        "00",
        "--- Chọn Báo Cáo Cần Rollback ---",
        [],
    ),
    (
        "all",
        "0. Toàn Bộ Hệ Thống (Rollback Tất Cả)",
        [],   # [] = lấy tất cả file_id từ config
    ),
    (
        "01",
        "1. Báo cáo Tín dụng & TSĐB  [bc_tin_dung_2026.xlsx + Hop_dong_tien_gui.xlsm]",
        ["fact_loan", "fact_collateral", "fact_credit_limit_summary", "fact_term_deposit"],
    ),
    (
        "02",
        "2. Báo cáo Tài chính B01  [B01 DN Bao cao tinh hinh tai chinh.xlsx]",
        ["fact_balance_sheet", "dim_report_item_b01"],
    ),
    (
        "03",
        "3. Báo cáo KQKD B02  [B02 DN Bao cao ket qua hoat dong kinh doanh.xlsx]",
        ["fact_income_statement", "dim_report_item_b02"],
    ),
    (
        "04",
        "4. Công nợ Phải Thu KH  [Chi_tiet_cong_no_phai_thu_khach_hang.xlsx]",
        ["fact_accounts_receivable"],
    ),
    (
        "05",
        "5. Công nợ Phải Trả NCC  [Chi_tiet_cong_no_phai_tra_nha_cung_cap.xlsx]",
        ["fact_accounts_payable"],
    ),
    (
        "06",
        "6. Sổ chi tiết Dòng tiền  [So_chi_tiet_cac_tai_khoan.xlsx]",
        ["fact_cashflow"],
    ),
    (
        "07",
        "7. Tổng hợp Tồn kho  [Tong_hop_ton_kho.xlsx]",
        ["fact_inventory_balance"],
    ),
    (
        "08",
        "8. Sổ chi tiết Mua hàng  [So_chi_tiet_mua_hang.xlsx]",
        ["fact_inventory_inward"],
    ),
    (
        "09",
        "9. Sổ chi tiết Bán hàng  [So_chi_tiet_ban_hang.xlsx]",
        ["fact_inventory_outward"],
    ),
    (
        "10",
        "10. Kế hoạch Kinh doanh  [Ke_hoach_kinh_doanh_minh_long_2026.xlsx]",
        ["fact_business_plan"],
    ),
    (
        "11",
        "11. Danh mục Đối tác  [Danh_sach_khach_hang.xlsx + Danh_sach_nha_cung_cap.xlsx]",
        ["dim_partner_khach_hang", "dim_partner_nha_cung_cap"],
    ),
    (
        "12",
        "12. Danh mục TK & Ngân hàng  [Danh_sach_ngan_hang.xlsx + he_thong_tai_khoan + tai_khoan_ngan_hang]",
        ["dim_account", "dim_account_number", "dim_bank"],
    ),
    (
        "13",
        "13. Danh mục Hàng hóa & Kho  [Danh_sach_hang_hoa_dich_vu.xlsx + Danh_sach_kho.xlsx]",
        ["dim_product", "dim_warehouse"],
    ),
]

_KEY_TO_FILE_IDS: dict[str, list[str]] = {k: ids  for k, _lbl, ids in _REPORTS}
_KEY_TO_LABEL:    dict[str, str]        = {k: lbl  for k, lbl, _ids in _REPORTS}

# Tương thích ngược: label cũ (chuỗi dài) → key ngắn
_LABEL_TO_KEY: dict[str, str] = {lbl: k for k, lbl, _ids in _REPORTS}


UI_PARAMS = {
    "report_name": Param(
        PLACEHOLDER_KEY,                             # default = "00" (key ngắn)
        type="string",
        enum=[k for k, _lbl, _ids in _REPORTS],     # enum = ["00","all","01",...]
        values_display={k: lbl for k, lbl, _ids in _REPORTS},
        description=(
            "Chọn báo cáo cần rollback. "
            "Trigger qua API dùng key ngắn: '01', '02', ... hoặc 'all'."
        ),
    ),
    "target_date": Param(
        None,
        type=["string", "null"],
        format="date",
        description=(
            "Ngày muốn rollback về (YYYY-MM-DD). "
            "Để trống = lấy batch gần nhất trước ngày HÔM NAY."
        ),
    ),
}


# =============================================================================
# DAG
# =============================================================================

@dag(
    dag_id="excel_pipeline_rollback",
    default_args=default_args,
    description="Rollback dữ liệu về batch cũ — dành cho BA / Vận hành",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params=UI_PARAMS,
    tags=["excel", "pipeline", "rollback"],
)
def excel_rollback_dag():

    @task
    def run_rollback(**context):
        import os
        import sys
        from datetime import date as _date

        os.chdir(PROJECT_ROOT)
        if PROJECT_ROOT not in sys.path:
            sys.path.insert(0, PROJECT_ROOT)

        from src.config_loader import load_config
        from src.tasks import rollback_task

        params  = context.get("params") or {}
        dag_run = context.get("dag_run")
        conf    = (dag_run.conf if dag_run else None) or {}

        # Resolve report key — chấp nhận key ngắn lẫn label cũ đầy đủ
        raw_key     = params.get("report_name") or conf.get("report_name") or PLACEHOLDER_KEY
        report_key  = _LABEL_TO_KEY.get(raw_key, raw_key)  # label cũ → key

        # Fix 4: validate sau khi resolve
        if report_key == PLACEHOLDER_KEY:
            raise AirflowFailException(
                "Vui lòng chọn một báo cáo cụ thể trước khi bấm Trigger! "
                f"Các key hợp lệ: {[k for k, _, _ in _REPORTS if k != PLACEHOLDER_KEY]}"
            )

        # Resolve target_date
        target_date_str = params.get("target_date") or conf.get("target_date")
        target_date = (
            _date.fromisoformat(target_date_str)
            if target_date_str
            else _date.today()
        )

        # Resolve file_ids
        config   = load_config(CONFIG_PATH)
        file_ids = _KEY_TO_FILE_IDS.get(report_key, [])

        if report_key == "all" or not file_ids:
            # Rollback toàn bộ — lấy tất cả file_id từ config
            file_ids = [f.file_id for f in config.excel_files]

        label = _KEY_TO_LABEL.get(report_key, report_key)
        import logging
        logger = logging.getLogger(__name__)
        logger.info(
            "[rollback] report=%s | key=%s | target_date=%s | file_ids=%s",
            label, report_key, target_date, file_ids,
        )

        results = rollback_task.run_batch(file_ids, target_date, config)

        failed = {
            fid: r.error
            for fid, r in results.items()
            if r.status == "error"
        }
        if failed:
            raise AirflowFailException(f"Rollback lỗi ở các bảng: {failed}")

        return {
            fid: {
                "status":              r.status,
                "rolled_back_to_date": str(r.rolled_back_to_date),
            }
            for fid, r in results.items()
        }

    run_rollback()


excel_rollback_dag()