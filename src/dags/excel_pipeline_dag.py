"""
excel_pipeline_dag.py - DAG chính: Nạp tự động (Auto) hoặc thủ công (Force Run).

KIẾN TRÚC WAVE (v2 — theo BÁO CÁO, không theo mã bảng):
  - Upload: chạy 1 lần / FILE VẬT LÝ (1 task upload_one xử lý mọi file_id
    dùng chung file đó, VD B01-DN chứa cả dim_report_item_b01 lẫn
    fact_balance_sheet — chỉ tính MD5/upload 1 lần).
  - Load: chia theo WAVE của BÁO CÁO (không phải mã bảng), đảm bảo đúng
    thứ tự phụ thuộc dim → fact đã xác nhận:
      Wave 0: Danh sách KH, Danh sách NH, Hệ thống TK, Kho, Hàng hóa,
              B01-DN, Kế hoạch KD (độc lập)
      Wave 1: Danh sách NCC, TK ngân hàng, B02-DN, Hợp đồng tiền gửi,
              Công nợ phải thu KH, BC Tín dụng, Sổ CT bán hàng,
              Tổng hợp tồn kho
      Wave 2: Sổ CT mua hàng, Sổ CT các tài khoản, Công nợ phải trả NCC

FIXES so với phiên bản cũ:
  Fix 1 — Import SDK Airflow 3.x (dag, task, Param từ airflow.sdk)
  Fix 2 — expand() trên list rỗng gây lỗi "no task to map" khi priority=[];
           dùng task chained thay vì expand trực tiếp trên list rỗng.
  Fix 3 — DAG run conf là dict, không dùng được context["dag_run"].conf khi
           chạy từ Scheduler (conf=None); đã thêm guard `or {}`.
  Fix 4 — Thiếu dependency giữa các wave khi dùng dynamic task mapping —
           dùng trigger_rule=ALL_DONE + chain thủ công giữa các wave.
"""
from __future__ import annotations
import logging
import uuid
import re
import calendar
from datetime import datetime, timedelta, date
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

try:
    from airflow.sdk import Variable
except ImportError:
    from airflow.models import Variable

PROJECT_ROOT = "/mnt/c/excel-pipeline"
CONFIG_PATH  = f"{PROJECT_ROOT}/config/pipeline_config.yaml"
LANDING_DIR  = "data/raw"

logger = logging.getLogger(__name__)


# =============================================================================
# WAVE THEO BÁO CÁO (không phải theo file_id/mã bảng)
# =============================================================================

ETL_WAVES_BY_REPORT: list[set[str]] = [
    # Wave 0 — độc lập, không phụ thuộc báo cáo nào khác
    {
        "danh_sach_khach_hang",
        "danh_sach_ngan_hang",
        "danh_sach_he_thong_tai_khoan",
        "danh_sach_kho",
        "danh_sach_hang_hoa_dich_vu",
        "b01_dn",
        "ke_hoach_kinh_doanh",
    },
    # Wave 1 — phụ thuộc 1 báo cáo ở Wave 0
    {
        "danh_sach_nha_cung_cap",
        "danh_sach_tai_khoan_ngan_hang",
        "b02_dn",
        "hop_dong_tien_gui",
        "chi_tiet_cong_no_phai_thu_kh",
        "bc_tin_dung",
        "so_chi_tiet_ban_hang",
        "tong_hop_ton_kho",
    },
    # Wave 2 — phụ thuộc 1 báo cáo ở Wave 1
    {
        "so_chi_tiet_mua_hang",
        "so_chi_tiet_cac_tai_khoan",
        "chi_tiet_cong_no_phai_tra_ncc",
    },
]

# Map mỗi file_id → report_key (báo cáo/file vật lý chứa nó) — để tra wave
FILE_ID_TO_REPORT: dict[str, str] = {
    "dim_partner_khach_hang":      "danh_sach_khach_hang",
    "dim_partner_nha_cung_cap":    "danh_sach_nha_cung_cap",
    "dim_bank":                    "danh_sach_ngan_hang",
    "dim_account_number":          "danh_sach_tai_khoan_ngan_hang",
    "dim_account":                 "danh_sach_he_thong_tai_khoan",
    "dim_warehouse":               "danh_sach_kho",
    "dim_product":                 "danh_sach_hang_hoa_dich_vu",
    "dim_report_item_b01":         "b01_dn",
    "fact_balance_sheet":          "b01_dn",
    "dim_report_item_b02":         "b02_dn",
    "fact_income_statement":       "b02_dn",
    "fact_inventory_outward":      "so_chi_tiet_ban_hang",
    "fact_inventory_inward":       "so_chi_tiet_mua_hang",
    "fact_inventory_balance":      "tong_hop_ton_kho",
    "fact_cashflow":               "so_chi_tiet_cac_tai_khoan",
    "fact_business_plan":          "ke_hoach_kinh_doanh",
    "fact_term_deposit":           "hop_dong_tien_gui",
    "fact_accounts_receivable":    "chi_tiet_cong_no_phai_thu_kh",
    "fact_accounts_payable":       "chi_tiet_cong_no_phai_tra_ncc",
    "fact_credit_limit_summary":   "bc_tin_dung",
    "fact_loan":                   "bc_tin_dung",
    "fact_collateral":             "bc_tin_dung",
}


def _wave_of_file_id(file_id: str) -> int:
    """Trả về index wave (0-based) của file_id, tra qua report chứa nó.
    Không map được -> wave cuối cùng (an toàn, chạy sau hết)."""
    report_key = FILE_ID_TO_REPORT.get(file_id)
    if report_key is None:
        logger.warning("[wave] file_id '%s' không map được report — đẩy vào wave cuối.", file_id)
        return len(ETL_WAVES_BY_REPORT) - 1
    for i, wave_set in enumerate(ETL_WAVES_BY_REPORT):
        if report_key in wave_set:
            return i
    logger.warning("[wave] report '%s' không khớp wave nào — đẩy vào wave cuối.", report_key)
    return len(ETL_WAVES_BY_REPORT) - 1


# 5 file_id thuộc 3 file nội bộ Google Sheets → không dùng bookmark MISA,
# fallback ngày upload (date.today()) trong upload_task.py
INTERNAL_GG_SHEET_FILE_IDS = {
    "fact_loan", "fact_collateral", "fact_credit_limit_summary", "fact_term_deposit",
    "fact_business_plan",
}


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
    ),
    "skip_load": Param(
        default=False,
        type="boolean",
        title="🧪 Test mode: Bỏ qua bước Load vào Database",
        description="Bật = chỉ chạy detect + upload MinIO, KHÔNG ghi vào PostgreSQL. "
                    "Dùng khi muốn test luồng mà không đụng dữ liệu.",
    ),
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


def _resolve_file_date(file_id: str, file_path: str | None = None):
    if file_id in INTERNAL_GG_SHEET_FILE_IDS:
        return None

    if file_path:
        m = re.search(r"_(\d{4})-(\d{2})\.xlsx$", Path(file_path).name)
        if m:
            y, mo = int(m.group(1)), int(m.group(2))
            today = date.today()
            if (y, mo) == (today.year, today.month):
                return today
            else:
                last_day = calendar.monthrange(y, mo)[1]
                return date(y, mo, last_day)

    try:
        bookmark = Variable.get("misa_last_to_date")
        return datetime.strptime(bookmark, "%d/%m/%Y").date()
    except Exception:
        logger.warning(
            "[upload] %s: không lấy được bookmark misa_last_to_date, fallback ngày hôm nay.",
            file_id,
        )
        return None


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
    # TASK 1: Quét file — trả về DANH SÁCH FILE VẬT LÝ (mỗi file giữ
    # nguyên toàn bộ file_ids liên quan, không tách theo mã bảng).
    # ─────────────────────────────────────────────────────────────────────
    @task
    def detect_changed_files(**context) -> dict[str, Any]:
        import os
        os.chdir(PROJECT_ROOT)

        load_config, match_files_in_dir, _, _ = _project_imports()

        params  = context.get("params") or {}
        dag_run = context.get("dag_run")
        conf    = (dag_run.conf if dag_run else None) or {}

        report_name  = params.get("report_name", AUTO_CHOICE)
        ui_file_ids  = set(_REPORT_MAP.get(report_name, []))
        extra_ids    = set(conf.get("file_ids") or [])
        only_file_ids = ui_file_ids | extra_ids  # rỗng = lấy tất cả

        config = load_config(CONFIG_PATH)

        all_matches: dict[str, list[str]] = {}
        for root, _dirs, _files in os.walk(LANDING_DIR):
            sub = match_files_in_dir(root, config)
            if sub:
                all_matches.update(sub)

        if not all_matches:
            logger.warning("Không tìm thấy file nào khớp trong %s", LANDING_DIR)

        files: list[dict] = []
        for file_path, file_ids in all_matches.items():
            matched_ids = [fid for fid in file_ids if not only_file_ids or fid in only_file_ids]
            if matched_ids:
                files.append({"file_path": file_path, "file_ids": matched_ids})

        logger.info("Phát hiện %d file vật lý cần upload.", len(files))
        return {"files": files}

    @task
    def extract_files(files_dict: dict) -> list[dict]:
        return files_dict.get("files") or []

    # ─────────────────────────────────────────────────────────────────────
    # TASK 2: Upload — 1 task / FILE VẬT LÝ. Xử lý mọi file_id dùng
    # chung file đó trong 1 lần chạy (VD B01-DN: dim_report_item_b01 +
    # fact_balance_sheet), trả về LIST kết quả upload cho từng file_id.
    # ─────────────────────────────────────────────────────────────────────
    @task(retries=2, retry_delay=timedelta(minutes=2))
    def upload_one(file_info: dict[str, Any], **context) -> list[dict[str, Any]]:
        import os
        os.chdir(PROJECT_ROOT)
        load_config, _, _, upload_task = _project_imports()

        params  = context.get("params") or {}
        dag_run = context.get("dag_run")
        conf    = (dag_run.conf if dag_run else None) or {}
        force   = _is_force_run(params, conf)

        config    = load_config(CONFIG_PATH)
        file_path = file_info["file_path"]
        results: list[dict[str, Any]] = []

        for file_id in file_info["file_ids"]:
            batch_id = f"{file_id}_{context['ts_nodash']}_{uuid.uuid4().hex[:8]}"
            result = upload_task.run(
                file_path=file_path,
                file_id=file_id,
                config=config,
                batch_id=batch_id,
                force_upload=force,
                file_date=_resolve_file_date(file_id, file_path),
            )
            logger.info(
                "[upload] %s | batch=%s | skipped=%s | path=%s | file_date=%s",
                result.file_id, result.batch_id, result.skipped, result.minio_path, result.file_date,
            )
            results.append({
                "file_id":    result.file_id,
                "batch_id":   result.batch_id,
                "minio_path": result.minio_path,
                "skipped":    result.skipped,
            })
        return results

    # ─────────────────────────────────────────────────────────────────────
    # Dàn phẳng kết quả upload (list of list -> list), rồi chia theo
    # WAVE của BÁO CÁO chứa từng file_id.
    # ─────────────────────────────────────────────────────────────────────
    @task
    def flatten_and_split_waves(upload_results: list[list[dict]]) -> dict[str, list[dict]]:
        flat = [item for sub in (upload_results or []) for item in (sub or [])]
        waves: list[list[dict]] = [[] for _ in ETL_WAVES_BY_REPORT]
        for item in flat:
            waves[_wave_of_file_id(item["file_id"])].append(item)
        logger.info(
            "Phân bổ load theo wave: %s",
            ", ".join(f"wave{i}={len(w)}" for i, w in enumerate(waves)),
        )
        return {f"wave_{i}": w for i, w in enumerate(waves)}

    @task
    def extract_wave(waves_dict: dict, wave_index: int) -> list[dict]:
        return waves_dict.get(f"wave_{wave_index}") or []

    # ─────────────────────────────────────────────────────────────────────
    # TASK 3: Load vào PostgreSQL — dùng chung cho mọi wave qua
    # .override(task_id=...) ở phần wire-up.
    # ─────────────────────────────────────────────────────────────────────
    @task(
        retries=1,
        retry_delay=timedelta(minutes=3),
        trigger_rule=TriggerRule.ALL_DONE,
    )
    def load_one(upload_result: dict[str, Any], **context) -> dict[str, Any]:
        import os
        os.chdir(PROJECT_ROOT)
        load_config, _, load_task, _ = _project_imports()

        params  = context.get("params") or {}

        if params.get("skip_load", False):
            logger.info(
                "[load] %s: 🧪 Test mode (skip_load=True) → bỏ qua load vào DB.",
                upload_result["file_id"],
            )
            return {"file_id": upload_result["file_id"], "status": "skipped_test_mode", "rows": 0}

        # ── ĐÃ BỎ: không còn check upload_result["skipped"] nữa ──
        # ETL luôn chạy, dùng minio_path/batch_id có sẵn (dù file MD5 không đổi
        # và không được upload lại lên MinIO).

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

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_prev_month_files(load_results: dict) -> None:
        """[CLEANUP] Chỉ xóa file tháng trước NẾU ETL không có lỗi
        (failed_count == 0) — tránh xóa file gốc khi load thất bại,
        mất cơ hội retry ở lần chạy sau."""
        if load_results.get("failed_count", 0) > 0:
            logger.warning(
                "[cleanup] Có %d file load lỗi — KHÔNG xóa file tháng trước, "
                "giữ lại để retry ở lần chạy kế tiếp.",
                load_results["failed_count"],
            )
            return

        MISA_RAW_DIR = Path("/mnt/c/excel-pipeline/data/raw/misa")
        today = date.today()
        cur_suffix = f"_{today.year:04d}-{today.month:02d}.xlsx"

        monthly_prefixes = [
            "B01_DN_Bao_cao_tinh_hinh_tai_chinh",
            "B02_DN_Bao_cao_ket_qua_hoat_dong_kinh_doanh",
            "Tong_hop_ton_kho",
        ]

        for f in MISA_RAW_DIR.glob("*_????-??.xlsx"):
            for prefix in monthly_prefixes:
                if f.name.startswith(prefix) and not f.name.endswith(cur_suffix):
                    try:
                        f.unlink()
                        logger.info(f"[cleanup] Đã xóa file tháng trước: {f.name}")
                    except Exception as e:
                        logger.warning(f"[cleanup] Không xóa được {f.name}: {e}")

    # ─────────────────────────────────────────────────────────────────────
    # TASK 4: Tổng kết — nhận list kết quả của TẤT CẢ wave.
    # ─────────────────────────────────────────────────────────────────────
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize(all_wave_results: list[list[dict]]) -> dict[str, Any]:
        all_results: list[dict] = []
        for wave_result in (all_wave_results or []):
            all_results.extend(list(wave_result) if wave_result else [])

        success = [r for r in all_results if r.get("status") == "success"]
        skipped = [r for r in all_results if r.get("status") in ("skipped", "skipped_test_mode")]
        failed  = [r for r in all_results if r.get("status") not in ("success", "skipped", "skipped_test_mode")]
        total_rows = sum(r.get("rows", 0) for r in success)

        logger.info("=" * 60)
        logger.info("TỔNG KẾT DAG RUN")
        logger.info("  ✓ Thành công : %d file | %d dòng", len(success), total_rows)
        logger.info("  ⏭ Bỏ qua    : %d file (MD5 không đổi / test mode)", len(skipped))
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
    # WIRE UP
    #
    #   detect → extract_files → upload_one[] (1/file vật lý)
    #                                  ↓
    #                       flatten_and_split_waves (chia theo báo cáo)
    #                                  ↓
    #     extract_wave(0) → load_one[] ──┐
    #     extract_wave(1) → load_one[] ──┤ (tuần tự, wave sau chờ wave trước)
    #     extract_wave(2) → load_one[] ──┘
    #                                  ↓
    #                             summarize → cleanup
    # ─────────────────────────────────────────────────────────────────────
    files_dict = detect_changed_files()
    file_items = extract_files(files_dict)

    uploaded   = upload_one.expand(file_info=file_items)
    waves_dict = flatten_and_split_waves(uploaded)

    loaded_per_wave = []
    prev_wave_loaded = None
    for i in range(len(ETL_WAVES_BY_REPORT)):
        wave_items = extract_wave.override(task_id=f"extract_wave_{i}")(
            waves_dict, wave_index=i
        )
        loaded = load_one.override(
            task_id=f"load_wave_{i}",
            trigger_rule=TriggerRule.ALL_DONE,
        ).expand(upload_result=wave_items)

        if prev_wave_loaded is not None:
            prev_wave_loaded >> wave_items  # wave sau chỉ tách khỏi waves_dict sau khi wave trước load xong

        loaded_per_wave.append(loaded)
        prev_wave_loaded = loaded

    result = summarize(all_wave_results=loaded_per_wave)
    cleanup_prev_month_files(load_results=result)


excel_pipeline_dag()