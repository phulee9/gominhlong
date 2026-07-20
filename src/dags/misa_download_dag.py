from __future__ import annotations

import logging
import subprocess
import sys                    # ← THÊM
from pathlib import Path      # ← THÊM
from datetime import datetime, timedelta

import requests

# ── THÊM: đảm bảo project root trong sys.path ─────────────────────────────
_PROJECT_ROOT = "/mnt/c/excel-pipeline"
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
# ──────────────────────────────────────────────────────────────────────────

from src.tasks import google_sheet_sync_task   # ← giờ import được rồi
from datetime import date, datetime, timedelta

try:
    from airflow.sdk import dag, task
    from airflow.sdk import Variable
except ImportError:
    from airflow.decorators import dag, task
    from airflow.models import Variable

try:
    from airflow.sdk import Param
except ImportError:
    from airflow.models.param import Param

logger = logging.getLogger(__name__)

WIN_PYTHON_EXE = "/mnt/d/Source/Automation/venv/Scripts/python.exe"
WIN_SCRIPT_PATH = "D:\\Source\\Automation\\report_download.py"
SCRIPT_TIMEOUT_SECONDS = 180 * 60
BOOKMARK_VAR = "misa_last_to_date"
AIRFLOW_API_URL = "http://localhost:8080/api/v1/dags/misa_download_pipeline"
AIRFLOW_USER = "admin"
AIRFLOW_PASS = "Inda1234"
PLACEHOLDER  = "2026-01-01"

REPORT_DISPLAY_NAMES: dict[str, str] = {
    "Danh sách khách hàng":                          "CUSTOMER_LIST",
    "Danh sách nhà cung cấp":                        "SUPPLIER_LIST",
    "Danh sách ngân hàng":                           "BANK_LIST",
    "Danh sách tài khoản ngân hàng":                 "BANK_ACCOUNT_LIST",
    "Danh sách hệ thống tài khoản":                  "COA_LIST",
    "Danh sách kho":                                 "WAREHOUSE_LIST",
    "Sổ chi tiết vật tư hàng hóa":                  "ITEM_LIST",
    "B01-DN: Báo cáo tình hình tài chính":           "B01_DN",
    "B02-DN: Báo cáo kết quả hoạt động kinh doanh":  "B02_DN",
    "Sổ chi tiết mua hàng":                          "PURCHASE_DETAIL",
    "Sổ chi tiết bán hàng":                          "SALES_DETAIL",
    "Tổng hợp tồn kho":                              "INV_SUMMARY_V2",
    "Sổ chi tiết các tài khoản":                     "GL_DETAIL_BY_ACCOUNT",
    "Chi tiết công nợ phải trả nhà cung cấp":        "AP_DETAIL",
    "Chi tiết công nợ phải thu khách hàng":          "AR_DETAIL",
}

ALL_REPORT_NAMES = list(REPORT_DISPLAY_NAMES.keys())

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

try:
    from airflow.exceptions import AirflowSkipException
except ImportError:
    from airflow.sdk.exceptions import AirflowSkipException

def parse_date_param(s: str) -> str:
    s = s.strip()
    if "/" in s:
        return datetime.strptime(s, "%d/%m/%Y").strftime("%d/%m/%Y")
    return datetime.strptime(s, "%Y-%m-%d").strftime("%d/%m/%Y")


@dag(
    dag_id="misa_download_pipeline",
    default_args=default_args,
    description="DAG tải báo cáo MISA | Chưa có thông tin lần chạy gần nhất",
    schedule="0 20 * * 4",
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    params={
        "use_bookmark": Param(
            default=True,
            type="boolean",
            title="Tự động dùng bookmark",
            description="Bật = tự tính khoảng ngày theo bookmark → hôm nay. "
                        "Tắt = dùng đúng 'Từ ngày'/'Đến ngày' nhập bên dưới.",
        ),
        "from_date": Param(
            default="2026-01-01",
            type="string",
            format="date",
            title="Từ ngày (chỉ áp dụng khi tắt bookmark)",
        ),
        "to_date": Param(
            default="2026-01-01",
            type="string",
            format="date",
            title="Đến ngày (chỉ áp dụng khi tắt bookmark)",
        ),

        # ════════════════════════════════════════════════════════════════
        # 📁 NHÓM 1 — BÁO CÁO MISA (có filter ngày tháng)
        # ════════════════════════════════════════════════════════════════
        "tai_khach_hang":   Param(default=True, type="boolean", title="📋 [MISA] Danh sách khách hàng"),
        "tai_nha_cung_cap": Param(default=True, type="boolean", title="📋 [MISA] Danh sách nhà cung cấp"),
        "tai_ngan_hang":    Param(default=True, type="boolean", title="📋 [MISA] Danh sách ngân hàng"),
        "tai_tk_ngan_hang": Param(default=True, type="boolean", title="📋 [MISA] Danh sách tài khoản ngân hàng"),
        "tai_he_thong_tk":  Param(default=True, type="boolean", title="📋 [MISA] Danh sách hệ thống tài khoản"),
        "tai_kho":          Param(default=True, type="boolean", title="📋 [MISA] Danh sách kho"),
        "tai_vthh":         Param(default=True, type="boolean", title="📋 [MISA] Danh sách vật tư hàng hoá"),
        "tai_b01":          Param(default=True, type="boolean", title="📊 [MISA] B01-DN: Tình hình tài chính"),
        "tai_b02":          Param(default=True, type="boolean", title="📊 [MISA] B02-DN: Kết quả kinh doanh"),
        "tai_mua_hang":     Param(default=True, type="boolean", title="🛒 [MISA] Sổ chi tiết mua hàng"),
        "tai_ban_hang":     Param(default=True, type="boolean", title="🛒 [MISA] Sổ chi tiết bán hàng"),
        "tai_ton_kho":      Param(default=True, type="boolean", title="🏭 [MISA] Tổng hợp tồn kho"),
        "tai_dong_tien":    Param(default=True, type="boolean", title="💰 [MISA] Sổ chi tiết các tài khoản"),
        "tai_cong_no_thu":  Param(default=True, type="boolean", title="📥 [MISA] Công nợ phải thu khách hàng"),
        "tai_cong_no_tra":  Param(default=True, type="boolean", title="📤 [MISA] Công nợ phải trả nhà cung cấp"),

        # ════════════════════════════════════════════════════════════════
        # 📁 NHÓM 2 — FILE NỘI BỘ từ Google Sheets (không cần chọn ngày)
        # ════════════════════════════════════════════════════════════════
        "gg_bc_tin_dung":       Param(default=True, type="boolean", title="🔗 [GG Sheets] BC Tín dụng"),
        "gg_ke_hoach_kd":       Param(default=True, type="boolean", title="🔗 [GG Sheets] Kế hoạch kinh doanh"),
        "gg_hop_dong_tien_gui": Param(default=True, type="boolean", title="🔗 [GG Sheets] Hợp đồng tiền gửi"),
    },
    catchup=False,
    tags=["misa", "download"],
)
def misa_download_dag():

    @task
    def run_misa_downloader(**context) -> str:
        params = context.get("params", {})
        today = datetime.now()

        try:
            bookmark = Variable.get(BOOKMARK_VAR)
        except KeyError:
            bookmark = None

        auto_from = bookmark or datetime(today.year, 1, 1).strftime("%d/%m/%Y")
        auto_to = today.strftime("%d/%m/%Y")

        use_bookmark = params.get("use_bookmark", True)

        if use_bookmark:
            from_date, to_date = auto_from, auto_to
        else:
            from_date = parse_date_param(params.get("from_date", auto_from))
            to_date = parse_date_param(params.get("to_date", auto_to))

        if datetime.strptime(from_date, "%d/%m/%Y") > datetime.strptime(to_date, "%d/%m/%Y"):
            raise SystemExit(
                f"Khoảng ngày không hợp lệ: Từ ngày ({from_date}) lớn hơn "
                f"Đến ngày ({to_date})."
            )

        logger.info("=== NGÀY THỰC TẾ SẼ CHẠY ===")
        logger.info("Từ ngày : %s", from_date)
        logger.info("Đến ngày: %s", to_date)
        logger.info("Bookmark: %s", bookmark or "chưa có — dùng đầu năm tài chính")
        logger.info("Chế độ  : %s", "Tự động (bookmark)" if use_bookmark else "Thủ công (nhập tay)")

        PARAM_TO_CODE: dict[str, str] = {
            "tai_khach_hang":   "CUSTOMER_LIST",
            "tai_nha_cung_cap": "SUPPLIER_LIST",
            "tai_ngan_hang":    "BANK_LIST",
            "tai_tk_ngan_hang": "BANK_ACCOUNT_LIST",
            "tai_he_thong_tk":  "COA_LIST",
            "tai_kho":          "WAREHOUSE_LIST",
            "tai_vthh":         "ITEM_LIST",
            "tai_b01":          "B01_DN",
            "tai_b02":          "B02_DN",
            "tai_mua_hang":     "PURCHASE_DETAIL",
            "tai_ban_hang":     "SALES_DETAIL",
            "tai_ton_kho":      "INV_SUMMARY_V2",
            "tai_dong_tien":    "GL_DETAIL_BY_ACCOUNT",
            "tai_cong_no_thu":  "AR_DETAIL",
            "tai_cong_no_tra":  "AP_DETAIL",
        }

        selected_codes = [
            code
            for param_key, code in PARAM_TO_CODE.items()
            if params.get(param_key, True)
        ]

        if not selected_codes:
            logger.info("Không có báo cáo MISA nào được chọn — bỏ qua task này.")
            raise AirflowSkipException("Không có báo cáo MISA nào được chọn.")

        logger.info("Báo cáo sẽ tải (%d): %s", len(selected_codes), ", ".join(selected_codes))

        cmd = [
            WIN_PYTHON_EXE, "-X", "utf8", WIN_SCRIPT_PATH,
            "--from-date", from_date,
            "--to-date",   to_date,
            "--reports",   ",".join(selected_codes),
            "--use-bookmark", "true" if use_bookmark else "false",   # ← THÊM
        ]
        logger.info("Lệnh chạy: %s", " ".join(cmd))

        stderr_lines: list[str] = []
        with subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        ) as proc:
            for line in proc.stdout:
                logger.info(line.rstrip())

            stderr_output = proc.stderr.read()
            if stderr_output:
                for line in stderr_output.splitlines():
                    logger.error("[STDERR] %s", line)
                stderr_lines = stderr_output.splitlines()

            try:
                proc.wait(timeout=SCRIPT_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                proc.kill()
                raise RuntimeError(f"Script vượt quá timeout {SCRIPT_TIMEOUT_SECONDS // 60} phút.")

            if proc.returncode != 0:
                raise RuntimeError(
                    f"Script thoát với code {proc.returncode}.\n"
                    f"Stderr: {chr(10).join(stderr_lines[-50:])}"
                )

        
        # Sửa thành: lưu ngày hôm sau để lần sau from không bị trùng
        _bm_next = (datetime.strptime(to_date, "%d/%m/%Y") + timedelta(days=1)).strftime("%d/%m/%Y")
        Variable.set(BOOKMARK_VAR, _bm_next)
        logger.info("Đã cập nhật bookmark → %s (= %s + 1 ngày)", _bm_next, to_date)

        return to_date

    # ── THÊM: task sync Google Sheets ─────────────────────────────────────
    @task(task_id="sync_google_sheets")
    def sync_google_sheets(**context):
        """Tải file nội bộ từ Google Sheets — chỉ tải file được chọn."""
        params = context.get("params", {})

        # Map param key → tên sheet trong GOOGLE_SHEETS config
        PARAM_TO_SHEET: dict[str, str] = {
            "gg_bc_tin_dung":       "BC Tín dụng",
            "gg_ke_hoach_kd":       "Kế hoạch kinh doanh",
            "gg_hop_dong_tien_gui": "Hợp đồng tiền gửi",
        }

        selected_sheets = [
            name
            for param_key, name in PARAM_TO_SHEET.items()
            if params.get(param_key, True)
        ]

        if not selected_sheets:
            logger.info("[sync_google_sheets] Không có file Google Sheets nào được chọn — bỏ qua.")
            raise AirflowSkipException("Không có file Google Sheets nào được chọn.")

        logger.info("[sync_google_sheets] Sẽ tải: %s", ", ".join(selected_sheets))

        # Filter danh sách trước khi truyền vào task
        _gg = google_sheet_sync_task
        original = _gg.GOOGLE_SHEETS
        _gg.GOOGLE_SHEETS = [s for s in original if s["name"] in selected_sheets]
        try:
            result = _gg.run(**context)
        finally:
            _gg.GOOGLE_SHEETS = original  # restore lại sau khi chạy xong

        return result
    # ──────────────────────────────────────────────────────────────────────

    @task(trigger_rule="all_done")  # chạy miễn misa_task đã kết thúc (dù success hay skip), không quan tâm gg_task
    def update_dag_description(to_date: str | None) -> None:
        if not to_date:
            logger.info("Bỏ qua cập nhật description — task MISA đã bị skip.")
            return
        new_description = (
            f"DAG tải báo cáo MISA tự động | "
            f"Lần chạy gần nhất đến: {to_date} | "
            f"Lần chạy tiếp theo sẽ bắt đầu từ: {to_date}"
        )
        try:
            resp = requests.patch(
                AIRFLOW_API_URL,
                json={"description": new_description},
                auth=(AIRFLOW_USER, AIRFLOW_PASS),
                timeout=10,
            )
            if resp.ok:
                logger.info("Đã cập nhật description DAG → %s", new_description)
            else:
                logger.warning("Cập nhật thất bại: %s %s", resp.status_code, resp.text)
        except Exception as e:
            logger.warning("Không cập nhật được description: %s", e)

    # ── Wire-up: 2 task chạy song song, cùng kết thúc trước update_desc ──
    misa_task = run_misa_downloader()
    gg_task   = sync_google_sheets()
    update_dag_description(misa_task)

    # ──────────────────────────────────────────────────────────────────────


misa_download_dag()