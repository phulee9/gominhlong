"""
================================
CHẠY NATIVE TRÊN WINDOWS (không qua WSL) — cài Python + Playwright trực
tiếp trên Windows.

Dùng "persistent context" của Playwright: một profile Chrome RIÊNG, do
chính Playwright tạo và quản lý (không phải Chrome bạn dùng hàng ngày).

CÁCH DÙNG:
  (1) Cài đặt (1 lần):
        pip install playwright openpyxl
        playwright install chromium

  (2) Đăng nhập lần đầu (có giao diện, tự nhập OTP nếu được hỏi):
        python misa_report_downloader_win.py --setup

  (3) Chạy tự động (headless, dùng lại session đã đăng nhập ở bước 2):
        python misa_report_downloader_win.py

  (4) Debug có giao diện:
        python misa_report_downloader_win.py --show

  (5) Ép tải lại (bỏ qua skip file còn mới trong 6h — file tháng ĐÃ CHỐT
      vẫn được giữ nguyên, muốn tải lại tháng đã chốt thì xóa file đó):
        python misa_report_downloader_win.py --force

Khi nào cần chạy lại --setup: khi script báo "Session hết hạn". Session
MISA tự hết hạn định kỳ phía server — đây KHÔNG phải lỗi script.

CẢNH BÁO PROFILE: TUYỆT ĐỐI không mở profile automation
(misa_automation_profile) bằng tiến trình thứ hai — kể cả playwright
codegen hay chạy tay trùng giờ Airflow. Chromium khác phiên bản/khác OS
(WSL) mở profile Windows sẽ làm hỏng cookie (mã hóa DPAPI) → mất đăng
nhập. Cần codegen thì dùng profile riêng:
  playwright codegen --user-data-dir="D:\\Source\\Automation\\codegen_profile" <url>

────────────────────────────────────────────────────────────────────────
BẢN SỬA (đánh dấu [FIX 1]..[FIX 10]):
  [FIX 1] download_timeout mặc định 300s; report nặng override riêng
          "download_timeout_ms" (AR_DETAIL = 600s).
  [FIX 2] Bỏ expect_popup lồng quanh click export (bug bấm 2 lần). Tab
          phụ do listener page.on("popup") tự đóng; click đúng 1 lần.
  [FIX 3] dismiss_message_overlay() ngay đầu export; hover fail → đóng
          overlay + hover lại 1 lần rồi mới dispatch_event.
  [FIX 4] Chờ 'Chọn tất cả trên bảng' 15s → 3s (fast-fail).
  [FIX 5] Sau 'Xem báo cáo': chờ toolbar export hiện (cap 90s) thay cho
          networkidle 180s.
  [FIX 6] Retry TỪNG task tối đa 3 lần; screenshot lỗi vào _errors/;
          reset dashboard giữa các lần; session hết hạn → dừng cả run.
  [FIX 7] Idempotent: file hợp lệ tải trong 6h gần nhất → skip (Airflow
          retry chỉ tải lại phần lỗi). --force để ép tải lại.
  [FIX 8] Validate file sau tải (openpyxl, ≥2 dòng, >5KB) — hỏng → retry.
  [FIX 9] Gate cuối: thiếu bất kỳ task nào → exit 1 (Airflow fail+retry).
  [FIX 10] CHẾ ĐỘ MONTHLY cho B01_DN, B02_DN, INV_SUMMARY_V2 khi
          --use-bookmark true:
          - Mỗi tháng 1 file: <tên_chuẩn>_YYYY-MM.xlsx (cùng thư mục).
          - Tháng hiện tại: tải 01→hôm nay, GHI ĐÈ file tháng đó mỗi run.
          - Tháng đã qua "CHƯA CHỐT SỔ" → tải lại TRỌN tháng, ghi đè.
            Quy tắc chốt sổ (không cần state file): file tháng M là FINAL
            khi mtime ≥ ngày 01 tháng M+1 — vì to-date luôn = ngày chạy,
            file ghi TRONG tháng M không thể chứa các ngày cuối tháng M.
          - Nhờ đó: lần chạy cuối của tháng KHÔNG cần rơi đúng ngày cuối
            tháng — lần chạy ĐẦU TIÊN của tháng sau tự quay lại chốt sổ
            tháng trước. Nghỉ nhiều tháng cũng tự tải bù đủ.
          - --use-bookmark false: giữ nguyên hành vi cũ — 1 file tên
            chuẩn, khoảng ngày đúng theo --from-date/--to-date.
          - File tháng ĐÃ CHỐT là bất biến: --force không tải lại; muốn
            tải lại thì xóa file tháng đó rồi chạy.
"""

import argparse
import asyncio
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from playwright.async_api import async_playwright

# Profile riêng cho automation — KHÔNG phải Chrome bạn dùng hàng ngày.
PROFILE_DIR = Path(r"D:\Source\Automation\misa_automation_profile")

MISA_OUTPUT_DIR = Path(r"C:\excel-pipeline\data\raw\misa")

# [FIX 11] MISA phát session cookie — profile không tự giữ được đăng nhập.
COOKIES_FILE = Path(r"D:\Source\Automation\misa_cookies.json")
# [FIX 6] Screenshot mỗi lần 1 task lỗi — mở ảnh là thấy màn hình MISA lúc đó.
ERROR_DIR = MISA_OUTPUT_DIR / "_errors"

# [FIX 1] Timeout chờ download mặc định: 5 phút.
DEFAULT_DOWNLOAD_TIMEOUT_MS = 300_000

# [FIX 4] Chờ nút 'Chọn tất cả trên bảng' tối đa 3s.
SELECT_ALL_WAIT_MS = 3_000

# [FIX 6] Số lần thử lại tối đa cho MỖI task trong 1 lần chạy script.
MAX_ATTEMPTS = 3

# [FIX 7] File tải trong N giờ gần nhất + hợp lệ → skip.
FRESH_FILE_MAX_AGE_HOURS = 0

# [FIX 8] File nhỏ hơn ngưỡng này coi như hỏng/rỗng.
MIN_FILE_SIZE_BYTES = 5_000

# [FIX 10] 3 report tải THEO THÁNG khi bookmark bật.
MONTHLY_REPORTS = {"B01_DN", "B02_DN", "INV_SUMMARY_V2"}

STANDARD_FILENAME: dict[str, str] = {
    "CUSTOMER_LIST":        "Danh_sach_khach_hang.xlsx",
    "SUPPLIER_LIST":        "Danh_sach_nha_cung_cap.xlsx",
    "BANK_LIST":            "Danh_sach_ngan_hang.xlsx",
    "BANK_ACCOUNT_LIST":    "Danh_sach_tai_khoan_ngan_hang.xlsx",
    "COA_LIST":             "Danh_sach_he_thong_tai_khoan.xlsx",
    "WAREHOUSE_LIST":       "Danh_sach_kho.xlsx",
    "ITEM_LIST":            "Danh_sach_hang_hoa_dich_vu.xlsx",
    "B01_DN":               "B01_DN_Bao_cao_tinh_hinh_tai_chinh.xlsx",
    "B02_DN":               "B02_DN_Bao_cao_ket_qua_hoat_dong_kinh_doanh.xlsx",
    "PURCHASE_DETAIL":      "So_chi_tiet_mua_hang.xlsx",
    "SALES_DETAIL":         "So_chi_tiet_ban_hang.xlsx",
    "INV_SUMMARY_V2":       "Tong_hop_ton_kho.xlsx",
    "GL_DETAIL_BY_ACCOUNT": "So_chi_tiet_cac_tai_khoan.xlsx",
    "AP_DETAIL":            "Chi_tiet_cong_no_phai_tra_nha_cung_cap.xlsx",
    "AR_DETAIL":            "Chi_tiet_cong_no_phai_thu_khach_hang.xlsx",
}

MISA_LOGIN_URL = "https://aspapp.misa.vn/App/Account/Join"
MISA_APP_BASE_URL = "https://actasp.misa.vn"
MISA_DASHBOARD_URL = f"{MISA_APP_BASE_URL}/app/dashboard/workbench"

EXPORT_BUTTON_SELECTOR = ".mi-v2-export"

DETAIL_REPORT_EXPORT_SELECTOR = (
    "div:nth-child(4) > .con-ms-tooltip > .tooltip-content > div > "
    ".dropdown-list-layout > .ms-component.ms-button.ms-button-size-default."
    "ms-button-secondary.ms-button-secondary-disabled-false.ms-button-radius-true."
    "ms-button-color-neutral.ms-dropdown__left"
)
B01_DN_EXPORT_SELECTOR = ".flex-center.print-button > .con-ms-tooltip > .tooltip-content > div > .ms-component"
B02_DN_EXPORT_SELECTOR = (
    ".ms-component.ms-button.ms-button-size-default.ms-button-secondary."
    "ms-button-secondary-disabled-false.ms-button-radius-true"
)

DETAIL_REPORT_EXPORT_HOVER_SELECTOR = "div:nth-child(4) > .con-ms-tooltip"
B01_DN_EXPORT_HOVER_SELECTOR = ".flex-center.print-button > .con-ms-tooltip"

ACCOUNTING_FIRM_NAME_HINT = "KIỂM TOÁN DTH"
COMPANY_NAME_HINT = "CÔNG TY CỔ PHẦN TẬP ĐOÀN GỖ"

REPORTS_TO_DOWNLOAD: list[dict] = [
    {
        "report_code": "CUSTOMER_LIST",
        "inbox_subfolder": "danh_sach_khach_hang",
        "menu_path": [
            {"name": "Danh mục"},
            {"name": "Khách hàng", "exact": True},
        ],
    },
    {
        "report_code": "SUPPLIER_LIST",
        "inbox_subfolder": "danh_sach_nha_cung_cap",
        "menu_path": [
            {"name": "Danh mục"},
            {"name": "Nhà cung cấp", "exact": True},
        ],
    },
    {
        "report_code": "BANK_LIST",
        "inbox_subfolder": "danh_sach_ngan_hang",
        "menu_path": [
            {"name": "Danh mục"},
            {"name": "Ngân hàng", "exact": True},
        ],
    },
    {
        "report_code": "BANK_ACCOUNT_LIST",
        "inbox_subfolder": "danh_sach_tai_khoan_ngan_hang",
        "menu_path": [
            {"name": "Danh mục"},
            {"name": "Tài khoản ngân hàng"},
        ],
    },
    {
        "report_code": "COA_LIST",
        "inbox_subfolder": "danh_sach_he_thong_tai_khoan",
        "menu_path": [
            {"name": "Danh mục"},
            {"name": "Hệ thống tài khoản"},
        ],
    },
    {
        "report_code": "WAREHOUSE_LIST",
        "inbox_subfolder": "danh_sach_kho",
        "menu_path": [
            {"name": "Danh mục"},
            {"name": "Kho", "index": 1},
        ],
    },
    {
        "report_code": "ITEM_LIST",
        "inbox_subfolder": "danh_sach_hang_hoa_dich_vu",
        "report_url": f"{MISA_APP_BASE_URL}/app/DI/DIInventoryItems",
        "skip_false_button": True,
    },
    {
        "report_code": "B01_DN",
        "inbox_subfolder": "b01_dn_bao_cao_tinh_hinh_tai_chinh",
        "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/GLB01_DN",
        "menu_path": [
            {"name": "Báo cáo", "exact": True},
            {"name": "Báo cáo tài chính", "method": "text", "exact": True},
            {"name": "B01-DN: Báo cáo tình hình tài"},
        ],
        "needs_params": True,
        "view_report_first": True,
        "export_selector": B01_DN_EXPORT_SELECTOR,
        "export_hover_selector": B01_DN_EXPORT_HOVER_SELECTOR,
        "wait_after_nav_ms": 3000,
        "date_panel_open": True,
        # [FIX 10] Ngày tải giờ do task-builder quyết định (monthly khi
        # bookmark bật / manual khi tắt) — bỏ force_from_year_start cũ.
    },
    {
        "report_code": "B02_DN",
        "inbox_subfolder": "b02_dn_bao_cao_ket_qua_hdkd",
        "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/GLB02_DN",
        "menu_path": [
            {"name": "Báo cáo", "exact": True},
            {"name": "Báo cáo tài chính", "method": "text", "exact": True},
            {"name": "B02-DN: Báo cáo kết quả hoạt"},
        ],
        "needs_params": True,
        "view_report_first": True,
        "export_selector": B02_DN_EXPORT_SELECTOR,
        "wait_after_nav_ms": 3000,
        "date_panel_open": True,
    },
    {
        "report_code": "PURCHASE_DETAIL",
        "inbox_subfolder": "so_chi_tiet_mua_hang",
        "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/PUDetailPurchaseByInventoryItemDynamic",
        "needs_params": True,
        "needs_select_all": True,
        "view_report_first": True,
        "skip_false_button": True,
        "export_selector": DETAIL_REPORT_EXPORT_SELECTOR,
        "export_hover_selector": DETAIL_REPORT_EXPORT_HOVER_SELECTOR,
    },
    {
        "report_code": "SALES_DETAIL",
        "inbox_subfolder": "so_chi_tiet_ban_hang",
        "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/SalesBookDetailDefault",
        "needs_params": True,
        "view_report_first": True,
        "needs_select_all": True,
        "skip_false_button": True,
        "export_selector": DETAIL_REPORT_EXPORT_SELECTOR,
        "export_hover_selector": DETAIL_REPORT_EXPORT_HOVER_SELECTOR,
    },
    {
        "report_code": "AP_DETAIL",
        "inbox_subfolder": "chi_tiet_cong_no_phai_tra_ncc",
        "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/PULiabilitiesDetailsToSupplier",
        "needs_params": True,
        "view_report_first": True,
        "skip_false_button": True,
        "export_selector": DETAIL_REPORT_EXPORT_SELECTOR,
        "export_hover_selector": DETAIL_REPORT_EXPORT_HOVER_SELECTOR,
    },
    {
        "report_code": "AR_DETAIL",
        "inbox_subfolder": "chi_tiet_cong_no_phai_thu_kh",
        "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/ReceivableDeptDetail",
        "needs_params": True,
        "view_report_first": True,
        "skip_false_button": True,
        "export_selector": DETAIL_REPORT_EXPORT_SELECTOR,
        "export_hover_selector": DETAIL_REPORT_EXPORT_HOVER_SELECTOR,
        # [FIX 1] Report nặng — server sinh file lâu, cho riêng 10 phút.
        "download_timeout_ms": 600_000,
    },
    {
        "report_code": "INV_SUMMARY_V2",
        "inbox_subfolder": "tong_hop_ton_kho",
        "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/INInventoryBalanceSummary",
        "menu_path": [
            {"name": "Báo cáo", "exact": True},
            {"name": "Kho", "method": "text", "index": 2},
            {"name": "Tổng hợp tồn kho", "exact": True},
        ],
        "needs_params": True,
        "needs_select_all": True,
        "view_report_first": True,
        "export_selector": DETAIL_REPORT_EXPORT_SELECTOR,
        "export_hover_selector": DETAIL_REPORT_EXPORT_HOVER_SELECTOR,
    },
    {
        "report_code": "GL_DETAIL_BY_ACCOUNT",
        "inbox_subfolder": "so_chi_tiet_cac_tai_khoan",
        "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/GLAccountLedger",
        "menu_path": [
            {"name": "Báo cáo", "exact": True},
            {"name": "Tổng hợp", "method": "text"},
            {"name": "Sổ chi tiết các tài khoản"},
        ],
        "needs_params": True,
        "needs_select_all": True,
        "combo_selections": [
            {
                "combo_selector": ".w-1\\/3.m-r-6 > .ms-combo > span > .con-ms-tooltip > .tooltip-content > .combo-main-content > .combo-actions > .btn-dropdown > .mi-v2",
                "value": "TH",
            },
        ],
        "view_report_first": True,
        "export_selector": DETAIL_REPORT_EXPORT_SELECTOR,
        "export_hover_selector": DETAIL_REPORT_EXPORT_HOVER_SELECTOR,
    },
]

TEST_PARAMS_ONLY = False
if TEST_PARAMS_ONLY:
    REPORTS_TO_DOWNLOAD = [r for r in REPORTS_TO_DOWNLOAD if r.get("needs_params")]

TEST_ONLY_REPORTS: list[str] = []
if TEST_ONLY_REPORTS:
    REPORTS_TO_DOWNLOAD = [r for r in REPORTS_TO_DOWNLOAD if r["report_code"] in TEST_ONLY_REPORTS]


class SessionExpiredError(RuntimeError):
    """[FIX 6] Session MISA hết hạn giữa chừng — retry vô nghĩa, dừng cả
    run và yêu cầu chạy lại --setup."""


# ─────────────────────────────────────────────────────────────────────────
# [FIX 7]/[FIX 8]/[FIX 10] Idempotent skip + validate + monthly helpers
# ─────────────────────────────────────────────────────────────────────────

def is_fresh_and_valid(path: Path, hours: int = FRESH_FILE_MAX_AGE_HOURS) -> bool:
    """[FIX 7] File tồn tại, đủ lớn, mới tải trong `hours` giờ → skip."""
    try:
        st = path.stat()
        return st.st_size > MIN_FILE_SIZE_BYTES and (time.time() - st.st_mtime) < hours * 3600
    except OSError:
        return False


def validate_xlsx(path: Path, min_rows: int = 2) -> None:
    """[FIX 8] Kiểm tra file thật sự dùng được; fail → raise để retry."""
    if not path.exists():
        raise RuntimeError(f"File không tồn tại sau khi lưu: {path.name}")
    size = path.stat().st_size
    if size < MIN_FILE_SIZE_BYTES:
        raise RuntimeError(f"File quá nhỏ ({size} bytes) — nghi export rỗng/hỏng: {path.name}")
    try:
        from openpyxl import load_workbook
    except ImportError:
        print(f"    [validate] CẢNH BÁO: chưa cài openpyxl — chỉ kiểm tra size ({size} bytes OK).")
        return
    try:
        wb = load_workbook(path, read_only=True)
        rows = wb.active.max_row or 0
        wb.close()
    except Exception as e:
        raise RuntimeError(f"File không mở được bằng openpyxl (hỏng?): {path.name} — {e}")
    if rows < min_rows:
        raise RuntimeError(f"{path.name} chỉ có {rows} dòng — export rỗng?")
    print(f"    [validate] OK — {path.name}: {rows} dòng, {size:,} bytes.")


def monthly_path(code: str, y: int, m: int) -> Path:
    """[FIX 10] Đường dẫn file tháng: <tên_chuẩn>_YYYY-MM.xlsx (file
    phẳng, cùng thư mục — sort tên = sort thời gian)."""
    stem, ext = STANDARD_FILENAME[code].rsplit(".", 1)
    return MISA_OUTPUT_DIR / f"{stem}_{y:04d}-{m:02d}.{ext}"


def is_month_final(path: Path, y: int, m: int) -> bool:
    """[FIX 10] File tháng M là FINAL (đã chốt sổ) khi được ghi SAU khi
    tháng M kết thúc: mtime >= 00:00 ngày 01 tháng M+1. Vì to-date luôn
    = ngày chạy, file ghi TRONG tháng M không thể chứa dữ liệu các ngày
    cuối tháng M — nên mtime tự nói lên tính đầy đủ, không cần state."""
    try:
        st = path.stat()
    except OSError:
        return False
    nxt = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    threshold = datetime(nxt.year, nxt.month, nxt.day).timestamp()
    return st.st_size > MIN_FILE_SIZE_BYTES and st.st_mtime >= threshold


def build_month_jobs(code: str, from_d: date, today: date) -> list[dict]:
    """[FIX 10] Danh sách job theo tháng, từ from_d đến hôm nay:
      - Tháng hiện tại: 01 (hoặc from_d nếu muộn hơn) → hôm nay, luôn
        tải lại mỗi run (ghi đè bản partial cũ).
      - Tháng đã qua CHƯA chốt sổ: tải TRỌN tháng (chốt sổ), ghi đè.
      - Tháng đã qua ĐÃ chốt sổ: bỏ qua — bất biến.
    Nhờ vậy lần chạy đầu của tháng mới tự quay lại chốt tháng trước;
    nghỉ nhiều tháng cũng tự tải bù đủ, không phụ thuộc lịch chạy."""
    jobs: list[dict] = []
    y, m = from_d.year, from_d.month
    while (y, m) <= (today.year, today.month):
        first = date(y, m, 1)
        nxt = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
        seg_from = max(first, from_d)
        dest = monthly_path(code, y, m)
        if (y, m) == (today.year, today.month):
            jobs.append({"y": y, "m": m, "dest": dest, "final": False,
                         "from": seg_from, "to": today})
        elif not is_month_final(dest, y, m):
            jobs.append({"y": y, "m": m, "dest": dest, "final": True,
                         "from": seg_from, "to": nxt - timedelta(days=1)})
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return jobs


# ─────────────────────────────────────────────────────────────────────────
# [FIX 2] Tự đóng mọi tab phụ (popup) — đăng ký 1 LẦN trên page công ty.
# ─────────────────────────────────────────────────────────────────────────
async def inject_saved_cookies(context) -> None:
    """Bơm cookie đã lưu vào context TRƯỚC khi vào MISA."""
    import json
    try:
        cookies = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"[cookie] CẢNH BÁO: chưa có {COOKIES_FILE.name} — cần chạy "
              f"'Đăng nhập MISA' (dang_nhap_misa.py) trước.")
        return
    except Exception as e:
        print(f"[cookie] CẢNH BÁO: không đọc được {COOKIES_FILE.name}: {e}")
        return
    try:
        await context.add_cookies(cookies)
        print(f"[cookie] đã bơm {len(cookies)} cookie từ {COOKIES_FILE.name}.")
    except Exception as e:
        print(f"[cookie] CẢNH BÁO: bơm cookie lỗi: {e}")


async def export_cookies(context) -> None:
    """Xuất cookie mới nhất ra JSON sau khi đăng nhập OK — tự gia hạn."""
    import json
    try:
        cookies = await context.cookies()
        COOKIES_FILE.write_text(
            json.dumps(cookies, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        print(f"[cookie] đã xuất lại {len(cookies)} cookie mới nhất -> {COOKIES_FILE.name}.")
    except Exception as e:
        print(f"[cookie] CẢNH BÁO: xuất cookie lỗi: {e}")


def register_popup_autoclose(page) -> None:
    async def _close(popup):
        try:
            await popup.wait_for_load_state("domcontentloaded", timeout=5_000)
        except Exception:
            pass
        try:
            await popup.close()
            print("    [popup] có mở tab phụ — đã tự đóng.")
        except Exception:
            pass

    page.on("popup", lambda p: asyncio.create_task(_close(p)))


async def dismiss_message_overlay(page) -> None:
    overlay_selectors = [
        "#message-box .ms-message-bg",
        ".ms-popup--background",
        ".ms-popup",
    ]

    for sel in overlay_selectors:
        try:
            overlay = page.locator(sel).first
            if await overlay.count() > 0 and await overlay.is_visible():
                print(f"    [overlay] phát hiện '{sel}', đang đóng...")
                closed = False
                for label in ["Đóng", "Close", "OK", "Đồng ý", "×", "X", "Bỏ qua"]:
                    try:
                        await page.get_by_role("button", name=label).first.click(timeout=2000)
                        closed = True
                        print(f"    [overlay] đã bấm nút '{label}'.")
                        break
                    except Exception:
                        continue
                if not closed:
                    for close_sel in [".ms-popup .ms-icon-close",
                                      ".ms-popup button.close",
                                      ".ms-popup .btn-close"]:
                        try:
                            btn = page.locator(close_sel).first
                            if await btn.count() > 0 and await btn.is_visible():
                                await btn.click(timeout=2000)
                                closed = True
                                print(f"    [overlay] đã bấm close '{close_sel}'.")
                                break
                        except Exception:
                            continue
                if not closed:
                    await page.keyboard.press("Escape")
                    print("    [overlay] đã nhấn Escape.")
                await page.wait_for_timeout(500)
        except Exception:
            pass


async def login_setup() -> None:
    """Chạy 1 lần, có giao diện, để bạn tự đăng nhập (kể cả OTP)."""
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=False,
            args=[
                "--start-maximized",
                "--disable-blink-features=AutomationControlled",
            ],
            viewport=None,
        )
        page = context.pages[0] if context.pages else await context.new_page()
        await page.goto(MISA_LOGIN_URL)

        print(">>> Một cửa sổ Chrome riêng (profile automation) vừa mở.")
        print(">>> Đăng nhập MISA bình thường (nhập OTP nếu được hỏi).")
        print(">>> Sau khi vào được màn hình chính, quay lại đây và nhấn Enter.")
        input()

        await context.close()
        print(f"\nĐã lưu profile đăng nhập vào: {PROFILE_DIR}")
        print("Từ giờ chạy 'python misa_report_downloader_win.py' (không cần --setup) để tự động tải báo cáo.")


async def click_first_visible(locator, index: int = 0, timeout: int = 10000, force: bool = False) -> None:
    """Bấm phần tử thứ `index` trong các phần tử ĐANG HIỂN THỊ khớp
    locator — bỏ qua bản sao ẩn trong tooltip của MISA."""
    count = await locator.count()
    visible_indices = []
    for i in range(count):
        if await locator.nth(i).is_visible():
            visible_indices.append(i)
    if index >= len(visible_indices):
        raise RuntimeError(
            f"Không đủ phần tử HIỂN THỊ khớp (cần index={index}, chỉ thấy "
            f"{len(visible_indices)} phần tử hiển thị trong tổng {count} "
            f"phần tử khớp text — phần còn lại có thể là tooltip ẩn)."
        )
    real_index = visible_indices[index]
    await locator.nth(real_index).click(timeout=timeout, force=force)


async def navigate_menu(page, menu_path: list[dict]) -> None:
    for step_num, step in enumerate(menu_path, start=1):
        name = step["name"]
        exact = step.get("exact", False)
        index = step.get("index", 0)
        method = step.get("method", "link")

        print(f"    [menu {step_num}/{len(menu_path)}] đang bấm '{name}' (method={method}, exact={exact}, index={index})...")

        if method == "link":
            locator = page.get_by_role("link", name=name, exact=exact)
        elif method == "text":
            locator = page.get_by_text(name, exact=exact)
        elif method == "scoped_text":
            locator = page.locator(step["scope"]).get_by_text(name, exact=exact)
        else:
            raise ValueError(f"navigate_menu: method không hỗ trợ: {method}")

        last_err = None
        for attempt in range(3):
            try:
                await locator.first.wait_for(state="attached", timeout=15000)
                await page.wait_for_timeout(500)
                await click_first_visible(locator, index=index, timeout=30000)
                await page.wait_for_timeout(800)
                print(f"    [menu {step_num}/{len(menu_path)}] OK")
                last_err = None
                break
            except Exception as e:
                last_err = e
                print(f"    [menu {step_num}/{len(menu_path)}] thử lại ({attempt+1}/3)...")
                await page.wait_for_timeout(1500)
        if last_err:
            raise last_err


async def wait_report_ready(page, report_cfg: dict, timeout_ms: int = 300_000) -> None:
    """Đợi report load xong (form tham số / nút export hiện) — không dùng
    networkidle vì MISA có request nền chạy liên tục."""
    export_sel = report_cfg.get("export_selector", EXPORT_BUTTON_SELECTOR)
    if report_cfg.get("needs_params"):
        ready_selectors = [
            "button:has-text('Chọn tham số')",
            "input[placeholder='DD/MM/YYYY']",
        ]
    else:
        ready_selectors = [export_sel]

    deadline = time.monotonic() + timeout_ms / 1000
    while time.monotonic() < deadline:
        if report_cfg.get("needs_params"):
            try:
                if await page.get_by_role("textbox", name="DD/MM/YYYY").first.is_visible():
                    print("    [ready] form tham số đã render (thấy ô ngày).")
                    return
            except Exception:
                pass
            try:
                if await page.get_by_role("button", name="Chọn tham số").first.is_visible():
                    print("    [ready] form tham số đã render (thấy nút Chọn tham số).")
                    return
            except Exception:
                pass
        else:
            for sel in ready_selectors:
                try:
                    if await page.locator(sel).first.is_visible():
                        print(f"    [ready] report đã sẵn sàng (khớp '{sel}').")
                        return
                except Exception:
                    pass
        await page.wait_for_timeout(1000)
    print("    [ready] hết thời gian chờ report — vẫn thử thao tác tiếp.")


async def navigate_to_report(page, report_cfg: dict) -> None:
    if "report_url" in report_cfg:
        await page.goto(MISA_DASHBOARD_URL, timeout=120_000)
        await page.wait_for_timeout(1000)
        await page.goto(report_cfg["report_url"], timeout=120_000)
    else:
        await page.goto(MISA_DASHBOARD_URL, timeout=120_000)
        await navigate_menu(page, report_cfg["menu_path"])

    await wait_report_ready(page, report_cfg)

    extra = report_cfg.get("wait_after_nav_ms", 0)
    if extra > 0:
        print(f"    [nav] chờ thêm {extra//1000}s để trang ổn định...")
        await page.wait_for_timeout(extra)


def normalize_date_str(date_str: str) -> str:
    """Chuẩn hóa '7/6/2026' → '07/06/2026' (mask ô ngày MISA cần đủ số 0)."""
    dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
    return dt.strftime("%d/%m/%Y")


def get_period_dates() -> tuple[str, str]:
    """(từ_ngày, đến_ngày) DD/MM/YYYY: đầu năm tài chính → hôm nay."""
    today = date.today()
    fiscal_year_start = date(today.year, 1, 1)  # TODO: xác nhận lại nếu khác 01/01
    return fiscal_year_start.strftime("%d/%m/%Y"), today.strftime("%d/%m/%Y")


async def fill_date_field(locator, value: str) -> None:
    digits_only = value.replace("/", "")

    for attempt in range(3):
        await locator.click(click_count=3)
        await asyncio.sleep(0.1)
        await locator.press("Delete")
        await asyncio.sleep(0.1)

        await locator.press_sequentially(digits_only, delay=150)
        await asyncio.sleep(0.4)
        await locator.press("Tab")
        await asyncio.sleep(0.3)

        actual = (await locator.input_value()).strip()
        if actual == value:
            return
        print(f"    [date] lần {attempt+1}: gõ '{value}', đọc lại '{actual}' — thử lại...")

    raise RuntimeError(f"Không điền được ngày '{value}' sau 3 lần thử.")


async def set_date_range(page, from_date: str, to_date: str,
                          panel_already_open: bool = False) -> None:
    if not panel_already_open:
        print("    [date] đang bấm 'Chọn tham số' (nếu có)...")
        for attempt in range(15):
            try:
                btn = page.get_by_role("button", name="Chọn tham số")
                await btn.first.wait_for(state="visible", timeout=10000)
                await btn.first.click(timeout=5000)
                print("    [date] đã bấm 'Chọn tham số'.")
                await page.wait_for_timeout(800)
                break
            except Exception:
                if attempt < 14:
                    print(f"    [date] chưa thấy nút 'Chọn tham số' (lần {attempt+1}/15) — chờ tiếp...")
                    await page.wait_for_timeout(5000)
                else:
                    print("    [date] không thấy nút 'Chọn tham số' — panel có thể đã mở sẵn.")
    else:
        print("    [date] panel tham số đã mở sẵn — bỏ qua bước 'Chọn tham số'.")

    print(f"    [date] đang điền Từ ngày={from_date}, Đến ngày={to_date}...")
    date_inputs = page.get_by_role("textbox", name="DD/MM/YYYY")

    async def _date_visible():
        try:
            return await date_inputs.first.is_visible()
        except Exception:
            return False

    _dl = time.monotonic() + 30
    while time.monotonic() < _dl and not await _date_visible():
        for opener in ["Chọn tham số", "Tham số"]:
            try:
                await page.get_by_role("button", name=opener).first.click(timeout=1500)
                await page.wait_for_timeout(600)
                break
            except Exception:
                pass
        await page.wait_for_timeout(800)

    if not await _date_visible():
        raise RuntimeError("Không mở được ô ngày (panel tham số không hiện).")
    await fill_date_field(date_inputs.first, from_date)
    await fill_date_field(date_inputs.nth(1), to_date)
    print("    [date] đã điền xong.")

    actual_from = (await date_inputs.first.input_value()).strip()
    actual_to = (await date_inputs.nth(1).input_value()).strip()
    if actual_from != from_date or actual_to != to_date:
        print(
            f"    [CẢNH BÁO] Ô ngày có thể CHƯA điền đúng — mong đợi "
            f"{from_date} / {to_date}, đọc lại được '{actual_from}' / '{actual_to}'."
        )


async def select_combo_value(page, combo_open_selector: str, value_text: str, exact: bool = True) -> None:
    try:
        await page.locator(combo_open_selector).first.click(timeout=5000)
        await page.wait_for_timeout(300)
        await click_first_visible(page.get_by_text(value_text, exact=exact), timeout=3000)
        print(f"    [combo] đã chọn '{value_text}'.")
    except Exception as e:
        print(f"    [combo] LỖI chọn '{value_text}' (selector='{combo_open_selector}'): {e}")


async def click_mystery_false_button(page) -> None:
    try:
        await page.get_by_role("button", name="false").click(timeout=3000)
        print("    [false-button] đã bấm.")
    except Exception:
        print("    [false-button] không thấy — bỏ qua.")


async def get_dropdown_row_selected_states(page, code_only: bool = False) -> dict[str, bool]:
    code_cells = page.locator(".dropdown-item-td--text")
    selected_containers = page.locator(".selected-container")

    code_count = await code_cells.count()
    container_count = await selected_containers.count()

    codes: list[str] = []
    for i in range(code_count):
        text = (await code_cells.nth(i).inner_text()).strip()
        if not code_only or re.fullmatch(r"[A-Z]{2,6}", text):
            codes.append(text)

    states: dict[str, bool] = {}
    for i, code in enumerate(codes):
        if i < container_count:
            has_selected = await selected_containers.nth(i).locator(".selected").count() > 0
            states[code] = has_selected
        else:
            states[code] = False

    return states


async def select_all_in_multiselect_combo(page, combo_locator, code_only: bool = False) -> None:
    toggle = combo_locator.locator(".combo-actions > .btn-dropdown > .mi-v2")
    print("    [vthh] đang mở dropdown...")
    try:
        await toggle.click(timeout=3000)
    except Exception:
        print("    [vthh] không thấy dropdown này — bỏ qua (report không liên quan VTHH).")
        return

    await page.wait_for_timeout(300)

    row_states = await get_dropdown_row_selected_states(page, code_only=code_only)
    already_selected = [code for code, sel in row_states.items() if sel]

    options = page.locator(".dropdown-item-td--text")
    count = await options.count()
    clicked_labels: list[str] = []
    for i in range(count):
        try:
            text = (await options.nth(i).inner_text()).strip()
            if code_only and not re.fullmatch(r"[A-Z]{2,6}", text):
                continue
            if row_states.get(text, False):
                continue
            await options.nth(i).click(timeout=2000)
            await page.wait_for_timeout(150)
            clicked_labels.append(text)
        except Exception:
            pass

    try:
        await toggle.click(timeout=2000)
    except Exception:
        pass

    print(
        f"    Combo: đã có sẵn {sorted(already_selected)}, "
        f"vừa bấm thêm {clicked_labels or '(không có gì cần bấm thêm)'}."
    )


async def select_all_vthh_groups(page) -> None:
    vthh_combo = page.locator(".combo-main-con").first
    await select_all_in_multiselect_combo(page, vthh_combo, code_only=True)


async def select_all_items_in_table(page) -> None:
    try:
        await page.get_by_text(re.compile(r"Chọn tất cả")).first.click(timeout=3000)
        print("    [select-all-table] đã bấm 'Chọn tất cả'.")
        await page.wait_for_timeout(500)
        return
    except Exception:
        pass

    try:
        header_cb = page.locator("thead input[type='checkbox']").first
        if await header_cb.count() > 0 and not await header_cb.is_checked():
            await header_cb.check(timeout=3000)
            print("    [select-all-table] đã tích checkbox header.")
            await page.wait_for_timeout(500)
            return
    except Exception:
        pass

    try:
        checked = await page.locator("tbody input[type='checkbox']:checked").count()
    except Exception:
        checked = 0
    if checked == 0:
        raise RuntimeError(
            "Không chọn được Vật tư hàng hóa nào (không thấy nút 'Chọn tất cả' "
            "lẫn checkbox header). MISA sẽ chặn export — dừng report này."
        )
    print(f"    [select-all-table] đã có {checked} item được tích từ trước — OK.")


async def click_view_report_button(page, report_cfg: dict) -> None:
    """[FIX 5] Bấm 'Xem báo cáo' rồi chờ toolbar export hiện (cap 90s)."""
    print("    [view] đang bấm 'Xem báo cáo'...")
    await page.get_by_role("button", name="Xem báo cáo").click(timeout=90_000)
    print("    [view] đã bấm — đang chờ toolbar export hiện (tối đa 90s)...")

    ready_sel = (report_cfg.get("export_hover_selector")
                 or report_cfg.get("export_selector", EXPORT_BUTTON_SELECTOR))
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            if await page.locator(ready_sel).first.is_visible():
                print("    [view] toolbar export đã hiện — report sẵn sàng.")
                await page.wait_for_timeout(2_000)
                return
        except Exception:
            pass
        await page.wait_for_timeout(1_000)
    print("    [view] không thấy toolbar export sau 90s — vẫn thử tiếp.")


async def click_export_and_download(
    page,
    export_selector: str = EXPORT_BUTTON_SELECTOR,
    use_first: bool = True,
    hover_selector: str | None = None,
    force_click: bool = False,
    download_timeout: int = DEFAULT_DOWNLOAD_TIMEOUT_MS,   # [FIX 1]
    confirm_dialog_button: str | None = None,
    export_columns: list[str] | None = None,
):
    """[FIX 2]/[FIX 3] Bấm 'Xuất ra Excel' — click đúng 1 lần (click →
    force → dispatch_event, cái nào ăn thì dừng); tab phụ do listener
    popup tự đóng; overlay được dọn trước khi hover."""
    await dismiss_message_overlay(page)   # [FIX 3]

    if hover_selector:
        for collapse_sel in [".report-param .ms-icon-collapse",
                             ".report-param .btn-collapse",
                             "button[title='Thu gọn']"]:
            try:
                el = page.locator(collapse_sel).first
                if await el.count() > 0 and await el.is_visible():
                    await el.click(timeout=2000)
                    await page.wait_for_timeout(400)
                    print("    [export] đã thu gọn panel tham số.")
                    break
            except Exception:
                pass

        hovered = False
        for hv_try in range(2):   # [FIX 3] 2 lần × 8s
            try:
                print(f"    [export] đang hover vào '{hover_selector}' (lần {hv_try+1}/2)...")
                await page.locator(hover_selector).first.hover(timeout=8_000)
                await page.wait_for_timeout(400)
                hovered = True
                print("    [export] hover OK.")
                break
            except Exception:
                print("    [export] hover bị che — đóng overlay rồi thử lại...")
                await dismiss_message_overlay(page)
                await page.wait_for_timeout(300)
        if not hovered:
            print("    [export] hover thất bại — sẽ click bằng dispatch_event.")

    target = page.locator(export_selector)

    # ── TRƯỜNG HỢP A: có DIALOG chọn cột/xác nhận ────────────────────────
    if confirm_dialog_button:

        async def _dialog_open():
            for sel in ["text=Tùy chọn xuất ra Excel",
                        ".ms-dialog:visible", ".ms-modal:visible",
                        ".ms-popup:visible"]:
                try:
                    if await page.locator(sel).first.is_visible():
                        return True
                except Exception:
                    pass
            return False

        for attempt in range(3):
            print(f"    [export] bấm export ('{export_selector}') mở dialog (lần {attempt+1})...")
            try:
                await (target.first if use_first else target).click(timeout=10_000)
            except Exception:
                try:
                    await (target.first if use_first else target).click(force=True, timeout=8_000)
                except Exception:
                    pass
            _d = time.monotonic() + 8
            while time.monotonic() < _d:
                if await _dialog_open():
                    break
                await page.wait_for_timeout(500)
            if await _dialog_open():
                print("    [export] dialog đã mở.")
                break
        else:
            print("    [export] CẢNH BÁO: dialog không mở sau 3 lần bấm.")

        names = [confirm_dialog_button, "Đồng ý", "Đồng Ý", "Áp dụng",
                 "Xác nhận", "Thực hiện", "Kết xuất", "Xuất khẩu", "Xuất",
                 "Tải về", "Tải xuống", "Hoàn thành", "Tiếp tục", "OK", "Có"]
        names = [n for n in names if n]

        async def _find_confirm_button():
            for name in names:
                loc_by_name = [
                    page.get_by_role("button", name=name, exact=True),
                    page.get_by_role("button", name=name),
                    page.locator(f".ms-button:has-text('{name}')"),
                    page.locator(f".ms-dialog__footer :text-is('{name}')"),
                    page.locator(f".ms-popup :text-is('{name}')"),
                    page.get_by_text(name, exact=True),
                ]
                for loc in loc_by_name:
                    try:
                        if await loc.first.is_visible():
                            return loc.first, name
                    except Exception:
                        pass
            for sel in [
                ".ms-dialog .ms-button-primary",
                ".ms-dialog .ms-button-color-primary",
                ".ms-popup .ms-button-primary",
                ".ms-modal .ms-button-primary",
                ".ms-dialog__footer .ms-button:last-child",
                ".ms-popup-footer .ms-button:last-child",
            ]:
                try:
                    loc = page.locator(sel).first
                    if await loc.is_visible():
                        return loc, sel
                except Exception:
                    pass
            return None, None

        btn = None
        _dl = time.monotonic() + 30
        while time.monotonic() < _dl:
            btn, matched = await _find_confirm_button()
            if btn is not None:
                print(f"    [export] dialog hiện — nút xác nhận: {matched}")
                break
            await page.wait_for_timeout(1000)

        if btn is not None:
            for col in (export_columns or []):
                try:
                    row = page.locator("tr", has_text=col).first
                    cb = row.locator("input[type='checkbox']").first
                    if await cb.count() > 0 and not await cb.is_checked():
                        await cb.check(timeout=5_000)
                        print(f"    [export] đã tích cột '{col}'.")
                except Exception as ce:
                    print(f"    [export] không tích được cột '{col}' ({ce}).")

        print("    [export] chờ download...")
        async with page.expect_download(timeout=download_timeout) as download_info:
            if btn is not None:
                for how in ("click", "force", "dispatch"):
                    try:
                        if how == "click":
                            await btn.click(timeout=8_000)
                        elif how == "force":
                            await btn.click(force=True, timeout=8_000)
                        else:
                            await btn.dispatch_event("click")
                        print(f"    [export] đã bấm xác nhận ({how}).")
                        break
                    except Exception:
                        continue
            else:
                print("    [export] không thấy nút xác nhận — chờ download tự về.")
        print("    [export] download xong.")
        return await download_info.value

    # ── TRƯỜNG HỢP B: bấm export tải luôn (hầu hết report) ───────────────
    print(f"    [export] đang bấm export ('{export_selector}'), chờ download (tối đa {download_timeout//1000}s)...")
    async with page.expect_download(timeout=download_timeout) as download_info:
        try:
            await (target.first if use_first else target).click(timeout=15_000)
            print("    [export] đã click.")
        except Exception:
            try:
                await (target.first if use_first else target).click(force=True, timeout=5_000)
                print("    [export] đã click (force).")
            except Exception:
                await (target.first if use_first else target).dispatch_event("click")
                print("    [export] đã click bằng dispatch_event (xuyên lớp phủ).")
    print("    [export] download xong.")
    return await download_info.value


async def is_session_expired(page) -> bool:
    try:
        password_input = page.locator("input[type='password']")
        return await password_input.count() > 0
    except Exception:
        return False


async def click_company_card_button(page, name_hint: str, timeout: int = 10000) -> None:
    card = page.locator(".invite-company-ele").filter(
        has=page.locator(".name-company", has_text=name_hint)
    )
    await card.locator("button.access").click(timeout=timeout)


async def login_and_select_company(context):
    """Đăng nhập + chọn công ty (2 cấp), trả về page TAB công ty thật.
    Giữ nguyên logic bản gốc."""
    page = await context.new_page()
    await page.goto(MISA_LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)

    try:
        await page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass

    if await is_session_expired(page):
        raise SystemExit(
            "Session hết hạn ngay từ đầu. "
            "Chạy lại: python misa_report_downloader_win.py --setup"
        )

    await click_company_card_button(page, ACCOUNTING_FIRM_NAME_HINT)

    try:
        await page.get_by_role("button").filter(has_text=re.compile(r"^$")).first.click(
            timeout=3000
        )
        print("    Đã đóng banner quảng cáo.")
    except Exception:
        pass

    await page.locator(".customer-name").first.wait_for(state="visible", timeout=60000)
    customer_row = page.locator("tr").filter(
        has=page.locator(".customer-name", has_text=COMPANY_NAME_HINT)
    )
    customer_id = await customer_row.first.get_attribute("id", timeout=60000)

    if not customer_id:
        raise RuntimeError(
            f"Không tìm thấy dòng công ty khớp '{COMPANY_NAME_HINT}' để lấy "
            f"id — dừng lại, không bấm bừa vào công ty khác."
        )
    print(f"    Tìm thấy dòng công ty khớp, id={customer_id}.")

    async with page.expect_popup() as popup_info:
        await page.locator(f'.il-text[data-customerid="{customer_id}"]').first.click(
            force=True, timeout=10000
        )
    company_page = await popup_info.value

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        u = company_page.url
        if "callback" not in u and "actasp.misa.vn" in u:
            break
        await company_page.wait_for_timeout(1000)
    else:
        raise RuntimeError(
            f"Tab công ty không rời khỏi trang callback sau 3 phút "
            f"(url hiện tại: {company_page.url})."
        )
    try:
        await company_page.wait_for_load_state("domcontentloaded", timeout=60000)
    except Exception:
        pass
    await company_page.wait_for_timeout(2000)

    print(f"    Đã chọn công ty, chuyển sang tab mới: {company_page.url}")

    page_text = await company_page.content()
    if COMPANY_NAME_HINT.upper() not in page_text.upper():
        raise RuntimeError(
            f"Không xác nhận được đang ở đúng công ty '{COMPANY_NAME_HINT}' "
            f"sau khi chọn — DỪNG LẠI, không tiếp tục tải report để tránh lấy "
            f"nhầm dữ liệu công ty khác."
        )

    return company_page


# ─────────────────────────────────────────────────────────────────────────
# [FIX 6]/[FIX 10] Thân xử lý 1 TASK (1 report + 1 khoảng ngày + 1 file
# đích). Ngày tải do task-builder quyết định — hàm này chỉ thực thi.
# ─────────────────────────────────────────────────────────────────────────

async def process_one_task(
    page,
    report_cfg: dict,
    eff_from: str,
    eff_to: str,
    dest_path: Path,
) -> Path:
    """Điều hướng → điền tham số (eff_from→eff_to) → xem báo cáo → export
    → lưu vào dest_path → validate. Lỗi → raise để caller retry."""
    code = report_cfg["report_code"]

    await dismiss_message_overlay(page)

    print("    [nav] đang điều hướng tới report...")
    await navigate_to_report(page, report_cfg)
    print(f"    [nav] đã vào: {page.url}")

    if await is_session_expired(page):
        raise SessionExpiredError(
            "Bị đẩy về trang đăng nhập — session đã hết hạn, cần chạy lại --setup."
        )

    if report_cfg.get("needs_deselect_all"):
        print("    [deselect] đang bỏ chọn tất cả...")
        try:
            await page.wait_for_timeout(2000)
            bo_chon = page.get_by_role("button", name="Bỏ chọn")
            if await bo_chon.first.is_visible():
                await bo_chon.first.click(timeout=3000)
                await page.wait_for_timeout(500)
                print("    [deselect] đã bấm nút 'Bỏ chọn'.")
            else:
                header_cb = page.locator("thead input[type='checkbox']").first
                if await header_cb.count() > 0 and await header_cb.is_checked():
                    await header_cb.uncheck(timeout=3000)
                    await page.wait_for_timeout(500)
                    print("    [deselect] đã bỏ tích checkbox header.")
                else:
                    print("    [deselect] không tìm thấy nút bỏ chọn — bỏ qua.")
        except Exception as e:
            print(f"    [deselect] lỗi: {e}")

    if report_cfg.get("needs_params"):
        print(f"    [date-plan] {code}: {eff_from} → {eff_to} → {dest_path.name}")
        await set_date_range(page, eff_from, eff_to,
                             panel_already_open=report_cfg.get("date_panel_open", False))

        if report_cfg.get("has_vthh_filter"):
            await select_all_vthh_groups(page)
            try:
                await page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass
            await page.wait_for_timeout(800)

        for combo in report_cfg.get("combo_selections", []):
            await select_combo_value(page, combo["combo_selector"], combo["value"])

        if report_cfg.get("needs_select_all"):
            wait_ms = report_cfg.get("wait_before_select_all_ms", 0)
            if wait_ms > 0:
                print(f"    [select-all] chờ {wait_ms//1000}s để trang load...")
                await page.wait_for_timeout(wait_ms)
            print("    [select-all] đang bấm 'Chọn tất cả trên bảng'...")
            try:
                btn = page.get_by_text(re.compile(r"Chọn tất cả.*trên bảng"))
                await btn.first.wait_for(state="visible", timeout=SELECT_ALL_WAIT_MS)  # [FIX 4]
                await btn.first.click(timeout=3000)
                await page.wait_for_timeout(1000)
                print("    [select-all] đã bấm.")
            except Exception:
                print("    [select-all] không thấy 'Chọn tất cả trên bảng' — thử fallback...")
                try:
                    select_all = page.locator(".rp-grid-selector__select-all-group")
                    if await select_all.first.is_visible():
                        await select_all.first.click()
                        await page.wait_for_timeout(1000)
                        print("    [select-all] đã bấm (fallback selector).")
                    else:
                        print("    [select-all] report này không có nút chọn-tất-cả — bỏ qua.")
                except Exception as e2:
                    print(f"    [select-all] fallback cũng lỗi ({e2}) — bỏ qua.")

        if report_cfg.get("needs_item_selection"):
            await select_all_items_in_table(page)

    if not report_cfg.get("skip_false_button"):
        await click_mystery_false_button(page)

    if report_cfg.get("view_report_first"):
        await click_view_report_button(page, report_cfg)   # [FIX 5]

    await dismiss_message_overlay(page)

    export_selector = report_cfg.get("export_selector", EXPORT_BUTTON_SELECTOR)
    hover_selector = report_cfg.get("export_hover_selector")
    download = await click_export_and_download(
        page, export_selector, hover_selector=hover_selector,
        download_timeout=report_cfg.get("download_timeout_ms", DEFAULT_DOWNLOAD_TIMEOUT_MS),  # [FIX 1]
        confirm_dialog_button=report_cfg.get("export_confirm_button"),
        export_columns=report_cfg.get("export_columns"),
    )

    MISA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if dest_path.exists():
        dest_path.unlink()
        print(f"    [save] đã xóa file cũ: {dest_path.name}")
    await download.save_as(dest_path)

    validate_xlsx(dest_path)   # [FIX 8]

    print(f"[OK] {code} -> {dest_path}")
    return dest_path


def build_task_list(from_date: str, to_date: str, use_bookmark: bool) -> list[dict]:
    """[FIX 10] Trải REPORTS_TO_DOWNLOAD thành danh sách task:
      - Report thuộc MONTHLY_REPORTS + bookmark BẬT → 1 task/tháng
        (tháng hiện tại partial + các tháng chưa chốt sổ).
      - Còn lại (kể cả monthly report khi bookmark TẮT) → 1 task duy nhất,
        khoảng ngày from→to, file tên chuẩn — đúng hành vi cũ.
    Mỗi task: {cfg, from, to, dest, label, kind}."""
    today = date.today()
    from_d = datetime.strptime(from_date, "%d/%m/%Y").date()
    fmt = "%d/%m/%Y"

    tasks: list[dict] = []
    for cfg in REPORTS_TO_DOWNLOAD:
        code = cfg["report_code"]
        if use_bookmark and code in MONTHLY_REPORTS:
            for job in build_month_jobs(code, from_d, today):
                kind = "final" if job["final"] else "current"
                tasks.append({
                    "cfg": cfg,
                    "from": job["from"].strftime(fmt),
                    "to": job["to"].strftime(fmt),
                    "dest": job["dest"],
                    "label": f"{code}[{job['y']:04d}-{job['m']:02d}]",
                    "kind": kind,
                    "y": job["y"], "m": job["m"],
                })
        else:
            tasks.append({
                "cfg": cfg,
                "from": from_date,
                "to": to_date,
                "dest": MISA_OUTPUT_DIR / STANDARD_FILENAME[code],
                "label": code,
                "kind": "single",
            })
    return tasks


async def download_reports(
    headless: bool = True,
    from_date_arg: str | None = None,
    to_date_arg: str | None = None,
    use_bookmark: bool = True,
    force: bool = False,
) -> None:
    if not PROFILE_DIR.exists():
        raise SystemExit(
            "Chưa setup đăng nhập. Chạy: python misa_report_downloader_win.py --setup"
        )

    default_from, default_to = get_period_dates()
    try:
        from_date = normalize_date_str(from_date_arg) if from_date_arg else default_from
        to_date = normalize_date_str(to_date_arg) if to_date_arg else default_to
    except ValueError as e:
        raise SystemExit(
            f"--from-date/--to-date không đúng format DD/MM/YYYY (vd 07/06/2026): {e}"
        )
    print(f"Khoảng ngày gốc: {from_date} -> {to_date} | bookmark={'ON' if use_bookmark else 'OFF'}")

    MISA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ERROR_DIR.mkdir(parents=True, exist_ok=True)

    # [FIX 10] Monthly mode: dọn file TÊN CŨ của 3 report monthly để ETL
    # không đọc trùng (dữ liệu giờ nằm trong các file *_YYYY-MM.xlsx).
    if use_bookmark:
        for code in MONTHLY_REPORTS:
            if any(c["report_code"] == code for c in REPORTS_TO_DOWNLOAD):
                legacy = MISA_OUTPUT_DIR / STANDARD_FILENAME[code]
                if legacy.exists():
                    legacy.unlink()
                    print(f"[dọn dẹp] xóa file tên cũ (đã chuyển sang file theo tháng): {legacy.name}")

    tasks = build_task_list(from_date, to_date, use_bookmark)
    print(f"Tổng số task: {len(tasks)} "
          f"({sum(1 for t in tasks if t['kind'] == 'final')} chốt sổ tháng cũ, "
          f"{sum(1 for t in tasks if t['kind'] == 'current')} tháng hiện tại, "
          f"{sum(1 for t in tasks if t['kind'] == 'single')} thường)")

    errors: list[str] = []
    downloaded = 0
    skipped = 0
    session_dead = False

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=headless,
            slow_mo=0 if headless else 2000,
            args=(["--start-maximized"] if not headless else []) + [
                "--disable-blink-features=AutomationControlled",
            ],
            viewport=None if not headless else {"width": 1600, "height": 900},
        )

        await inject_saved_cookies(context)   # [FIX 11] TRƯỚC khi vào MISA

        page = await login_and_select_company(context)
        register_popup_autoclose(page)   # [FIX 2]

        await export_cookies(context)   # [FIX 11] session tự gia hạn

        await dismiss_message_overlay(page)
        await page.wait_for_timeout(500)

        for idx, task in enumerate(tasks):
            label = task["label"]
            dest = task["dest"]
            print(f"\n--- Đang xử lý: {label} ({task['from']} → {task['to']}) ---")

            # Skip rules:
            # - task chốt sổ: nếu file ĐÃ final (vd attempt trước của cùng
            #   run Airflow vừa chốt xong) → skip. File final là bất biến.
            # - task khác: file hợp lệ tải trong 6h gần nhất → skip
            #   (trừ khi --force).
            if task["kind"] == "final" and is_month_final(dest, task["y"], task["m"]):
                print(f"[SKIP] {label} — tháng đã chốt sổ ({dest.name}).")
                skipped += 1
                continue
            if task["kind"] != "final" and not force and is_fresh_and_valid(dest):
                print(f"[SKIP] {label} — file hợp lệ tải trong {FRESH_FILE_MAX_AGE_HOURS}h gần nhất "
                      f"({dest.name}). Dùng --force để ép tải lại.")
                skipped += 1
                continue

            for attempt in range(1, MAX_ATTEMPTS + 1):   # [FIX 6]
                try:
                    await process_one_task(page, task["cfg"], task["from"], task["to"], dest)
                    downloaded += 1
                    break

                except SessionExpiredError as e:
                    print(f"[FATAL] {label}: {e}")
                    errors.append(label)
                    session_dead = True
                    break

                except Exception as e:
                    print(f"[RETRY {attempt}/{MAX_ATTEMPTS}] {label}: {e}")

                    try:
                        if not page.is_closed():
                            shot = ERROR_DIR / f"{label.replace('[', '_').replace(']', '')}_attempt{attempt}.png"
                            await page.screenshot(path=str(shot), full_page=True)
                            print(f"    [debug] đã chụp màn hình lỗi: {shot}")
                    except Exception:
                        pass

                    if attempt == MAX_ATTEMPTS:
                        print(f"[ERROR] {label}: fail cả {MAX_ATTEMPTS} lần — bỏ qua, xử lý task tiếp theo.")
                        errors.append(label)
                        break

                    try:
                        if page.is_closed():
                            raise RuntimeError("tab report đã đóng")
                        await page.goto(MISA_DASHBOARD_URL, timeout=120_000)
                        await dismiss_message_overlay(page)
                        await page.wait_for_timeout(1000)
                    except Exception:
                        print("    [recover] tab report hỏng — đăng nhập/chọn công ty lại...")
                        page = await login_and_select_company(context)
                        register_popup_autoclose(page)
                        await dismiss_message_overlay(page)

            if session_dead:
                remaining = [t["label"] for t in tasks[idx + 1:]]
                if remaining:
                    print(f"[FATAL] Session hết hạn — bỏ qua các task còn lại: {remaining}")
                    errors.extend(remaining)
                print(">>> Chạy: python misa_report_downloader_win.py --setup  (đăng nhập lại), "
                      "sau đó chạy lại script — task đã xong sẽ tự skip.")
                break

        await context.close()

    print(f"\n=== TỔNG KẾT: tải mới {downloaded}, bỏ qua (đã có) {skipped}, "
          f"lỗi {len(errors)} / tổng {len(tasks)} task ===")
    if errors:
        print(f"Screenshot lỗi (nếu có) nằm trong: {ERROR_DIR}")
        raise SystemExit(f"Hoàn tất với lỗi ở: {errors}")   # [FIX 9]

    print(f"Xong — đủ toàn bộ {len(tasks)} task trong {MISA_OUTPUT_DIR}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Tự động tải báo cáo MISA")
    parser.add_argument(
        "--setup", action="store_true", help="Đăng nhập tay lần đầu (có giao diện)"
    )
    parser.add_argument(
        "--show", action="store_true", help="Chạy có giao diện để debug, thay vì headless"
    )
    parser.add_argument(
        "--from-date",
        type=str,
        default=None,
        help="Từ ngày (DD/MM/YYYY).",
    )
    parser.add_argument(
        "--to-date",
        type=str,
        default=None,
        help="Đến ngày (DD/MM/YYYY).",
    )
    parser.add_argument(
        "--reports",
        type=str,
        default=None,
        help="Danh sách report_code cần tải, phân cách bằng dấu phẩy. "
             "Không truyền = tải tất cả. VD: B01_DN,B02_DN",
    )
    parser.add_argument(
        "--use-bookmark",
        type=str,
        default="true",
        help="'true' = B01_DN/B02_DN/INV_SUMMARY_V2 tải THEO THÁNG "
             "(mỗi tháng 1 file *_YYYY-MM.xlsx, tháng hiện tại 01→hôm nay, "
             "tự chốt sổ tháng cũ ở lần chạy đầu của tháng mới). "
             "'false' = tải 1 file duy nhất theo --from-date/--to-date.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help=f"Tải lại kể cả khi file đã có và còn mới trong "
             f"{FRESH_FILE_MAX_AGE_HOURS}h. File tháng ĐÃ CHỐT SỔ vẫn được "
             f"giữ nguyên — muốn tải lại thì xóa file tháng đó.",
    )

    args = parser.parse_args()

    if args.reports:
        selected = [r.strip() for r in args.reports.split(",") if r.strip()]
        REPORTS_TO_DOWNLOAD[:] = [
            r for r in REPORTS_TO_DOWNLOAD
            if r["report_code"] in selected
        ]
        print(f"Chỉ tải {len(REPORTS_TO_DOWNLOAD)} report: {selected}")

    if args.setup:
        asyncio.run(login_setup())
    else:
        asyncio.run(
            download_reports(
                headless=False,
                # headless=not args.show,
                from_date_arg=args.from_date,
                to_date_arg=args.to_date,
                use_bookmark=(args.use_bookmark.strip().lower() == "true"),
                force=args.force,
            )
        )