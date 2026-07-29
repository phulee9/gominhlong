"""

================================
CHẠY NATIVE TRÊN WINDOWS (không qua WSL) — cài Python + Playwright trực
tiếp trên Windows.

Dùng "persistent context" của Playwright: một profile Chrome RIÊNG, do
chính Playwright tạo và quản lý (không phải Chrome bạn dùng hàng ngày,
không phải bản sao/clone của profile nào cả) — nên không gặp các rào cản
đã gặp trước đó (Chrome chặn debug profile mặc định, App-Bound Encryption
chặn cookie bị copy sang nơi khác...).

CÁCH DÙNG:
  (1) Cài đặt (1 lần):
        pip install playwright
        playwright install chromium

  (2) Đăng nhập lần đầu (có giao diện, tự nhập OTP nếu được hỏi):
        python misa_report_downloader_win.py --setup

  (3) Chạy tự động (headless, dùng lại session đã đăng nhập ở bước 2):
        python misa_report_downloader_win.py

  (4) Nếu cần xem trực quan để debug (không headless):
        python misa_report_downloader_win.py --show

Khi nào cần chạy lại --setup: khi script báo "Session hết hạn" — đăng
nhập tay lại 1 lần, không cố tự động hoá bước này.
"""

import argparse
import asyncio
import re
import sys
from datetime import date, datetime
from pathlib import Path

from playwright.async_api import async_playwright

# Profile riêng cho automation — KHÔNG phải Chrome bạn dùng hàng ngày.
PROFILE_DIR = Path(r"D:\Source\Automation\misa_automation_profile")

MISA_OUTPUT_DIR = Path(r"C:\excel-pipeline\data\raw\misa")

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


# class Tee:
#     """Ghi mọi print() ra ĐỒNG THỜI nhiều stream (vd terminal + file log),
#     để không phải sửa từng dòng print() rải rác khắp file."""

#     def __init__(self, *streams):
#         self.streams = streams

#     def write(self, data):
#         for s in self.streams:
#             s.write(data)

#     def flush(self):
#         for s in self.streams:
#             s.flush()

# Domain ĐĂNG NHẬP thật (xác nhận từ bạn: bị đá về đây nếu chưa đăng nhập).
MISA_LOGIN_URL = "https://aspapp.misa.vn/App/Account/Join"

# Domain APP của công ty SAU KHI đã bấm "Xem dữ liệu" — đây là nơi các
# report thật sự nằm (lấy từ address bar trong ảnh chụp thật).
MISA_APP_BASE_URL = "https://actasp.misa.vn"

# Trang dashboard "sạch" — dùng để RESET trạng thái sidebar trước mỗi lần
# điều hướng bằng menu_path. Cần thiết vì menu "Báo cáo" dùng accordion
# (mở ra không tự đóng lại) — nếu không reset, các mục mở từ report
# trước sẽ làm lệch index/đè lên link report sau (xác nhận qua lỗi thật:
# "intercepts pointer events" + lệch số phần tử hiển thị).
MISA_DASHBOARD_URL = f"{MISA_APP_BASE_URL}/app/dashboard/workbench"

# Selector nút "Xuất ra Excel" — lấy từ Inspect thật trên MISA ASP.
# Class "mi-v2-export" là icon dùng chung cho nút xuất Excel ở các report
# DANH MỤC (list đơn giản, không tham số).
EXPORT_BUTTON_SELECTOR = ".mi-v2-export"

# Các report "Báo cáo" (có chọn tham số, bấm "Xem báo cáo" trước khi export)
# dùng toolbar export KHÁC — ghi nhận thật từ codegen, dùng chung cho hầu
# hết report dạng "sổ chi tiết"/"công nợ"/"tồn kho".
DETAIL_REPORT_EXPORT_SELECTOR = (
    "div:nth-child(4) > .con-ms-tooltip > .tooltip-content > div > "
    ".dropdown-list-layout > .ms-component.ms-button.ms-button-size-default."
    "ms-button-secondary.ms-button-secondary-disabled-false.ms-button-radius-true."
    "ms-button-color-neutral.ms-dropdown__left"
)
# B01-DN/B02-DN (báo cáo tài chính) lại dùng toolbar export khác nữa.
B01_DN_EXPORT_SELECTOR = ".flex-center.print-button > .con-ms-tooltip > .tooltip-content > div > .ms-component"
B02_DN_EXPORT_SELECTOR = (
    ".ms-component.ms-button.ms-button-size-default.ms-button-secondary."
    "ms-button-secondary-disabled-false.ms-button-radius-true"
)

# Phần tử cha cần HOVER trước để lộ ra .tooltip-content chứa nút export
# thật bên trong — click thẳng vào export_selector khi chưa hover sẽ báo
# "element not visible" vì tooltip-content còn ẩn (chỉ hiện khi hover).
DETAIL_REPORT_EXPORT_HOVER_SELECTOR = "div:nth-child(4) > .con-ms-tooltip"
B01_DN_EXPORT_HOVER_SELECTOR = ".flex-center.print-button > .con-ms-tooltip"


# Tên công ty CẤP 1 (công ty kế toán/kiểm toán quản lý nhiều khách hàng)
# — lấy từ thuộc tính title trong span.name-company thật trên trang.
ACCOUNTING_FIRM_NAME_HINT = "KIỂM TOÁN DTH"

# Dùng để tìm đúng DÒNG công ty ở CẤP 2 (danh sách công ty mà kế toán
# quản lý) — khớp theo div.customer-name, sau đó lấy id của <tr> chứa nó
# để bấm chính xác nút "Xem dữ liệu" tương ứng (xem login_and_select_company).
COMPANY_NAME_HINT = "CÔNG TY CỔ PHẦN TẬP ĐOÀN GỖ"

# Khai báo từng report theo ĐƯỜNG DẪN MENU (click theo đúng thứ tự text
# hiển thị) — khớp với hướng dẫn bạn cung cấp. Nếu MISA dùng SPA (URL
# không đổi giữa các report), cách click theo menu ổn định hơn dùng URL.
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
        # TODO: CHƯA xác nhận qua codegen (không có trong lần ghi vừa rồi)
        # — vẫn để tạm theo đoán ban đầu, cần ghi lại để xác nhận exact/index.
        "menu_path": [
            {"name": "Danh mục"},
            {"name": "Tài khoản ngân hàng"},
        ],
    },
    {
        "report_code": "COA_LIST",  # Hệ thống tài khoản
        "inbox_subfolder": "danh_sach_he_thong_tai_khoan",
        "menu_path": [
            {"name": "Danh mục"},
            {"name": "Hệ thống tài khoản"},  # codegen: không cần exact
        ],
        # Report này codegen ghi nhận export bằng selector khác hẳn (positional,
        # dễ vỡ) — tạm vẫn dùng EXPORT_BUTTON_SELECTOR chung, theo dõi riêng
        # report này khi test nếu export không hoạt động.
    },
    {
        "report_code": "WAREHOUSE_LIST",
        "inbox_subfolder": "danh_sach_kho",
        "menu_path": [
            {"name": "Danh mục"},
            {"name": "Kho", "index": 1},  # codegen: KHÔNG exact, nhưng .nth(1)
        ],
    },
#     {
#     "report_code": "ITEM_LIST",
#     "inbox_subfolder": "danh_sach_hang_hoa_dich_vu",
#     "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/INInventoryBookDetail",
#     "menu_path": [
#         {"name": "Báo cáo", "exact": True},
#         {"name": "Kho", "method": "text", "index": 2},
#         {"name": "Sổ chi tiết vật tư hàng hóa", "exact": True},
#     ],
#     "needs_params": True,
#     # "has_vthh_filter": True,
#     "needs_select_all": True,
#     "view_report_first": True,
#     "wait_after_view_ms": 30000,  # chờ 30s sau khi bấm Xem báo cáo
#     "export_selector": DETAIL_REPORT_EXPORT_SELECTOR,
#     "export_hover_selector": DETAIL_REPORT_EXPORT_HOVER_SELECTOR,
# },
{
    "report_code": "ITEM_LIST",
    "inbox_subfolder": "danh_sach_hang_hoa_dich_vu",
    "report_url": f"{MISA_APP_BASE_URL}/app/DI/DIInventoryItems",
    "skip_false_button": True,  # ← phải có dòng này
},


    {
        "report_code": "B01_DN",
        "inbox_subfolder": "b01_dn_bao_cao_tinh_hinh_tai_chinh",
        "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/GLB01_DN",
        # menu_path để tham khảo/fallback nếu URL đổi:
        "menu_path": [
            {"name": "Báo cáo", "exact": True},
            {"name": "Báo cáo tài chính", "method": "text", "exact": True},
            {"name": "B01-DN: Báo cáo tình hình tài"},  # substring khớp đủ
        ],
        "needs_params": True,
        "view_report_first": True,
        "export_selector": B01_DN_EXPORT_SELECTOR,
        "export_hover_selector": B01_DN_EXPORT_HOVER_SELECTOR,
        "force_from_year_start": True, 
        "wait_after_nav_ms": 3000,  # thêm dòng này
        "date_panel_open": True,

    },
    {
        "report_code": "B02_DN",
        "inbox_subfolder": "b02_dn_bao_cao_ket_qua_hdkd",
        "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/GLB02_DN",
        "menu_path": [
            {"name": "Báo cáo", "exact": True},
            {"name": "Báo cáo tài chính", "method": "text", "exact": True},
            {"name": "B02-DN: Báo cáo kết quả hoạt"},  # substring khớp đủ
        ],
        "needs_params": True,
        "view_report_first": True,
        "export_selector": B02_DN_EXPORT_SELECTOR,
        "select_params_button": True,
        "force_from_year_start": True, 
        "wait_after_nav_ms": 3000,  # thêm dòng này
        "date_panel_open": True,
    },
    {
        "report_code": "PURCHASE_DETAIL",
        "inbox_subfolder": "so_chi_tiet_mua_hang",
        "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/PUDetailPurchaseByInventoryItemDynamic",
        "needs_params": True,
        "needs_select_all": True,
        "view_report_first": True,
        "skip_false_button": True,   # ← THÊM
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
        "skip_false_button": True,   # ← THÊM
        "export_selector": DETAIL_REPORT_EXPORT_SELECTOR,
        "export_hover_selector": DETAIL_REPORT_EXPORT_HOVER_SELECTOR,
    },
    {
        "report_code": "AP_DETAIL",
        "inbox_subfolder": "chi_tiet_cong_no_phai_tra_ncc",
        "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/PULiabilitiesDetailsToSupplier",
        "needs_params": True,
        # combo_selections ← BỎ TOÀN BỘ
        "view_report_first": True,   # ← THÊM
        "skip_false_button": True,
        "export_selector": DETAIL_REPORT_EXPORT_SELECTOR,
        "export_hover_selector": DETAIL_REPORT_EXPORT_HOVER_SELECTOR,
    },
    {
        "report_code": "AR_DETAIL",
        "inbox_subfolder": "chi_tiet_cong_no_phai_thu_kh",
        "report_url": f"{MISA_APP_BASE_URL}/app/RP/ReportList/RPDynamicViewer/ReceivableDeptDetail",
        "needs_params": True,
        # combo_selections ← BỎ TOÀN BỘ
        "view_report_first": True,   # ← THÊM
        "skip_false_button": True,
        "export_selector": DETAIL_REPORT_EXPORT_SELECTOR,
        "export_hover_selector": DETAIL_REPORT_EXPORT_HOVER_SELECTOR,
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
        # "has_vthh_filter": True,
        "needs_select_all": True,
        "view_report_first": True,
        "export_selector": DETAIL_REPORT_EXPORT_SELECTOR,
        "export_hover_selector": DETAIL_REPORT_EXPORT_HOVER_SELECTOR,
        # "export_confirm_button": "Đồng ý",  # export xong hiện dialog xác nhận
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
        "needs_select_all": True,  # thêm dòng này
        "combo_selections": [
            {
                "combo_selector": ".w-1\\/3.m-r-6 > .ms-combo > span > .con-ms-tooltip > .tooltip-content > .combo-main-content > .combo-actions > .btn-dropdown > .mi-v2",
                "value": "TH",  # Loại tiền = TH (tiền hạch toán)
            },
        ],
        "view_report_first": True,
        "export_selector": DETAIL_REPORT_EXPORT_SELECTOR,
        "export_hover_selector": DETAIL_REPORT_EXPORT_HOVER_SELECTOR,
    },
    
]

# Cờ TEST tạm — bật True để CHỈ chạy 8 report cần tham số (Báo cáo),
# bỏ qua 7 report Danh mục đơn giản, giúp test nhanh hơn khi đang debug
# phần tham số. Đổi lại False khi muốn chạy đủ toàn bộ 14 report.
TEST_PARAMS_ONLY = False
if TEST_PARAMS_ONLY:
    REPORTS_TO_DOWNLOAD = [r for r in REPORTS_TO_DOWNLOAD if r.get("needs_params")]
    
# Thêm ngay sau dòng TEST_PARAMS_ONLY
TEST_ONLY_REPORTS: list[str] = []  # để [] để chạy tất cả
if TEST_ONLY_REPORTS:
    REPORTS_TO_DOWNLOAD = [r for r in REPORTS_TO_DOWNLOAD if r["report_code"] in TEST_ONLY_REPORTS]

async def dismiss_message_overlay(page) -> None:
    # Danh sách các overlay/popup cần đóng
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
                    # Thử bấm nút X (close) trong popup
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
                    # Fallback: nhấn Escape
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
    """
    Bấm vào phần tử thứ `index` trong số các phần tử ĐANG HIỂN THỊ khớp
    locator — bỏ qua các phần tử trùng text nhưng đang ẩn.

    MISA dùng class "ms-tooltip" ở khắp nơi (mặc định display:none, chỉ
    hiện khi hover phần tử khác) để hiện tên đầy đủ khi text bị cắt ngắn
    — nội dung của nó thường TRÙNG Y NGUYÊN với text menu thật, nên nếu
    không lọc visibility, get_by_text/get_by_role có thể vô tình khớp
    đúng cái tooltip ẩn này trước, dẫn tới lỗi "element is not visible"
    dù phần tử thật cần bấm vẫn hiển thị bình thường.

    force: dùng khi phần tử thật bị 1 lớp phủ hiển thị (vd bản sao cột
    cố định của DataTables FixedColumns, class "DTFC_LeftWrapper",
    aria-hidden="true") đè lên, khiến Playwright từ chối click vì nghĩ
    đang click nhầm lớp phủ — trong trường hợp đó lớp phủ chỉ là hiệu
    ứng hình ảnh, ép click xuyên qua là an toàn.
    """
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
    """
    Click lần lượt theo từng bước trong menu_path. Mỗi bước là 1 dict:
      {"name": "<text hiển thị>", "exact": True/False, "index": N, "method": "link"/"text"/"scoped_text", "scope": "<css selector>"}
    "exact" mặc định False, "index" mặc định 0, "method" mặc định "link".
    Khớp đúng theo những gì Playwright codegen ghi nhận thật cho từng
    report — menu "Danh mục" dùng link (<a>), menu "Báo cáo" dùng text
    thường (không phải link), đôi khi cần scope vào #main-content để
    tránh khớp nhầm chỗ khác có cùng chữ trên trang.

    "index" được hiểu là index trong số các phần tử ĐANG HIỂN THỊ (xem
    click_first_visible) — bỏ qua các bản sao ẩn trong tooltip.
    """
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

        # Menu có thể chưa render xong sau khi reset dashboard -> chờ + retry.
        last_err = None
        for attempt in range(3):
            try:
                # chờ ít nhất 1 phần tử khớp xuất hiện trong DOM
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
    """
    Đợi đến khi report THẬT SỰ load xong, bất kể load lâu bao nhiêu.
    MISA có request nền chạy ngầm liên tục nên networkidle không đáng tin;
    thay vào đó chờ tới khi 1 trong các dấu hiệu report-đã-sẵn-sàng xuất
    hiện: form tham số, bảng dữ liệu, hoặc nút export. Poll đến timeout_ms
    (mặc định 5 phút) — load chậm thì cứ đợi tiếp, không bỏ ngang.
    """
    export_sel = report_cfg.get("export_selector", EXPORT_BUTTON_SELECTOR)
    # Chờ ĐÚNG phần tử sắp thao tác, không dùng selector chung chung
    # ([class*='report'] khớp cả URL/khung rỗng -> báo sẵn sàng quá sớm).
    if report_cfg.get("needs_params"):
        # report có tham số: phải thấy ô ngày HOẶC nút "Chọn tham số" mới là
        # form đã render xong.
        ready_selectors = [
            "button:has-text('Chọn tham số')",
            "textbox[name='DD/MM/YYYY']",  # placeholder ô ngày
            "input[placeholder='DD/MM/YYYY']",
        ]
    else:
        # report danh mục: chờ nút export thật hiện.
        ready_selectors = [export_sel]

    import time as _t
    deadline = _t.monotonic() + timeout_ms / 1000
    while _t.monotonic() < deadline:
        # ô ngày kiểm bằng get_by_role riêng (không phải CSS)
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
        # Reset về dashboard trước để đóng panel cũ, rồi mới goto report mới
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
    """
    Chuẩn hóa chuỗi ngày người dùng nhập (vd '7/6/2026') về đúng format
    DD/MM/YYYY có số 0 ở đầu (vd '07/06/2026'). Ô ngày MISA gõ theo từng
    ký tự với mask cố định DD/MM/YYYY — thiếu số 0 sẽ làm lệch hết các ký
    tự gõ sau, ra kết quả sai mà không báo lỗi rõ ràng.
    """
    dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
    return dt.strftime("%d/%m/%Y")


def get_period_dates() -> tuple[str, str]:
    """
    Trả về (từ_ngày, đến_ngày) dạng DD/MM/YYYY cho các report cumulative
    (từ đầu năm tài chính tới ngày chạy).
    TODO: đổi FISCAL_YEAR_START nếu năm tài chính Minh Long không trùng
    năm dương lịch (hiện đang giả định 01/01).
    """
    today = date.today()
    fiscal_year_start = date(today.year, 1, 1)  # TODO: xác nhận lại nếu khác 01/01
    return fiscal_year_start.strftime("%d/%m/%Y"), today.strftime("%d/%m/%Y")


async def fill_date_field(locator, value: str) -> None:
    digits_only = value.replace("/", "")  # "01/01/2026" → "01012026"
    
    for attempt in range(3):
        # click 3 lần để chọn hết text (thay triple_click không tồn tại)
        await locator.click(click_count=3)
        await asyncio.sleep(0.1)
        await locator.press("Delete")
        await asyncio.sleep(0.1)

        # Gõ chỉ số, bỏ "/" vì mask tự thêm
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
        # Loop tìm nút "Chọn tham số" như cũ
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

    # Ô ngày có thể chưa hiện (panel đóng lại). Chờ ngắn; nếu vẫn chưa có
    # thì bấm lại "Chọn tham số" để mở panel, tối đa vài lần.
    async def _date_visible():
        try:
            return await date_inputs.first.is_visible()
        except Exception:
            return False

    import time as _t
    _dl = _t.monotonic() + 30
    while _t.monotonic() < _dl and not await _date_visible():
        # thử mở lại panel
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

    # Xác minh lại — báo rõ nếu điền không đúng, không âm thầm bỏ qua.
    actual_from = (await date_inputs.first.input_value()).strip()
    actual_to = (await date_inputs.nth(1).input_value()).strip()
    if actual_from != from_date or actual_to != to_date:
        print(
            f"    [CẢNH BÁO] Ô ngày có thể CHƯA điền đúng — mong đợi "
            f"{from_date} / {to_date}, đọc lại được '{actual_from}' / '{actual_to}'."
        )


async def select_combo_value(page, combo_open_selector: str, value_text: str, exact: bool = True) -> None:
    """
    Mở 1 combo single-select (bấm nút mở dropdown ở combo_open_selector),
    rồi chọn đúng value_text trong list. Dùng cho "Tài khoản" (331/131),
    "Loại tiền" (TH)... — KHÁC với extra_clicks cũ (bấm thẳng vào text mà
    không mở dropdown trước, nên bấm hụt vì text chưa hiện ra).
    """
    try:
        await page.locator(combo_open_selector).first.click(timeout=5000)
        await page.wait_for_timeout(300)
        await click_first_visible(page.get_by_text(value_text, exact=exact), timeout=3000)
        print(f"    [combo] đã chọn '{value_text}'.")
    except Exception as e:
        print(f"    [combo] LỖI chọn '{value_text}' (selector='{combo_open_selector}'): {e}")


async def click_mystery_false_button(page) -> None:
    """
    Một số report (Khách hàng, Nhà cung cấp, Vật tư hàng hóa...) có 1 nút
    với accessible name literally là chữ "false" (có thể do MISA bind
    nhầm thuộc tính boolean vào label — không rõ ý nghĩa thật, chỉ biết
    cần bấm thì luồng export mới đúng). KHÔNG phải report nào cũng có nút
    này, nên luôn thử và bỏ qua nếu không thấy — vô hại nếu dư.
    """
    try:
        await page.get_by_role("button", name="false").click(timeout=3000)
        print("    [false-button] đã bấm.")
    except Exception:
        print("    [false-button] không thấy — bỏ qua.")


async def get_dropdown_row_selected_states(page, code_only: bool = False) -> dict[str, bool]:
    """
    Đọc trạng thái CHỌN/CHƯA CHỌN của từng dòng NGAY TRONG dropdown đang
    mở — đáng tin hơn đọc từ chip bên ngoài (chip có khung đếm số + icon
    "xem thêm" khi chọn nhiều, có thể KHÔNG hiển thị đủ từng item, làm
    hiểu lầm 1 item đã chọn thành chưa chọn — đúng là nguyên nhân mục
    cuối bị bấm tắt đi).

    Cấu trúc DOM thật xác nhận qua Inspect:
      - mỗi dòng có 1 td.selected-container: RỖNG (chỉ có comment Vue,
        <!---->) nếu CHƯA chọn; chứa div.selected nếu ĐÃ chọn.
      - thứ tự các td.selected-container khớp đúng thứ tự các dòng, cùng
        thứ tự với các cell .dropdown-item-td--text.

    Trả về dict {mã: đã_chọn (bool)}.
    """
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
            states[code] = False  # không khớp được số dòng — coi như chưa chọn, an toàn hơn bỏ sót

    return states


async def select_all_in_multiselect_combo(page, combo_locator, code_only: bool = False) -> None:
    """
    Mở 1 combo multi-select (list bên trong khi mở), đọc trạng thái
    CHỌN/CHƯA CHỌN của từng dòng NGAY TRONG dropdown (xem
    get_dropdown_row_selected_states), rồi chỉ bấm vào mã CHƯA được chọn
    — bấm = TOGGLE THẬT (đang tắt thì bật, đang bật thì tắt), nên TUYỆT
    ĐỐI không bấm lại mã đã chọn sẵn, sẽ làm tắt đi.

    code_only: 1 số dropdown (vd "Nhóm VTHH") hiển thị MỖI DÒNG thành 2
    cell cùng class .dropdown-item-td--text — 1 cell mã ngắn (vd "CCDC"),
    1 cell tên đầy đủ (vd "Công cụ dụng cụ"). Khi code_only=True, chỉ xét
    cell dạng MÃ NGẮN VIẾT HOA (regex [A-Z]{2,6}), bỏ qua cell tên đầy đủ.

    Nếu trang không có combo này (report không liên quan), bỏ qua toàn
    bộ, không coi là lỗi.
    """
    toggle = combo_locator.locator(".combo-actions > .btn-dropdown > .mi-v2")
    print("    [vthh] đang mở dropdown...")
    try:
        await toggle.click(timeout=3000)
    except Exception:
        print("    [vthh] không thấy dropdown này — bỏ qua (report không liên quan VTHH).")
        return  # không có dropdown này trên report này

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
                continue  # cell tên đầy đủ — bỏ qua
            if row_states.get(text, False):
                continue  # ĐÃ chọn sẵn — KHÔNG bấm lại, bấm = toggle sẽ làm tắt đi
            await options.nth(i).click(timeout=2000)
            await page.wait_for_timeout(150)  # đợi UI cập nhật trước khi bấm dòng tiếp theo
            clicked_labels.append(text)
        except Exception:
            pass

    # Đóng dropdown bằng cách bấm LẠI đúng nút đã mở — KHÔNG dùng phím
    # Escape, vì 1 số dropdown custom coi Escape là "HỦY" (revert lại toàn
    # bộ lựa chọn vừa tích), không phải chỉ đóng lại.
    try:
        await toggle.click(timeout=2000)
    except Exception:
        pass

    print(
        f"    Combo: đã có sẵn {sorted(already_selected)}, "
        f"vừa bấm thêm {clicked_labels or '(không có gì cần bấm thêm)'}."
    )


async def select_all_vthh_groups(page) -> None:
    """Tích hết 'Nhóm vật tư hàng hóa' — mỗi dòng có 2 cell (mã + tên đầy
    đủ) cùng class, nên chỉ xét cell mã ngắn (code_only=True)."""
    vthh_combo = page.locator(".combo-main-con").first
    await select_all_in_multiselect_combo(page, vthh_combo, code_only=True)


async def select_all_items_in_table(page) -> None:
    """
    Bấm "Chọn tất cả N trên bảng" để chọn hết dòng dữ liệu (mã hàng, nhà
    cung cấp, khách hàng, tài khoản...) trong report. Số N động theo mỗi
    report nên dùng regex khớp linh hoạt; nếu đã ở trạng thái "Đã chọn
    tất cả" từ trước (không có nút "Chọn tất cả" để bấm nữa), bỏ qua,
    không coi là lỗi.
    """
    # Thử bấm "Chọn tất cả N trên bảng"
    try:
        await page.get_by_text(re.compile(r"Chọn tất cả")).first.click(timeout=3000)
        print("    [select-all-table] đã bấm 'Chọn tất cả'.")
        await page.wait_for_timeout(500)
        return
    except Exception:
        pass

    # Không thấy nút "Chọn tất cả" — kiểm tra đã có item được tích chưa.
    # Nếu CHƯA có item nào tích -> tích ô checkbox 'chọn tất cả' ở header bảng.
    try:
        header_cb = page.locator("thead input[type='checkbox']").first
        if await header_cb.count() > 0 and not await header_cb.is_checked():
            await header_cb.check(timeout=3000)
            print("    [select-all-table] đã tích checkbox header.")
            await page.wait_for_timeout(500)
            return
    except Exception:
        pass

    # Xác nhận thật sự đã có ít nhất 1 item tích; nếu không -> báo lỗi ngay
    # thay vì để MISA chặn bằng popup rồi export rỗng.
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


async def click_view_report_button(page) -> None:
    print("    [view] đang bấm 'Xem báo cáo'...")
    await page.get_by_role("button", name="Xem báo cáo").click(timeout=90_000)
    print("    [view] đã bấm — đang chờ report load xong (có thể lâu)...")
    try:
        await page.wait_for_load_state("networkidle", timeout=180_000)
    except Exception:
        pass
    print("    [view] report đã load xong.")


async def click_export_and_download(
    page,
    export_selector: str = EXPORT_BUTTON_SELECTOR,
    use_first: bool = True,
    hover_selector: str | None = None,
    force_click: bool = False,
    download_timeout: int = 1_200_000,   # nới cho report load lâu
    confirm_dialog_button: str | None = None,  # nút "Đồng ý" của dialog chọn cột (ITEM_LIST)
    export_columns: list[str] | None = None,   # tên các cột cần tích trong dialog
):
    """
    Bấm nút "Xuất ra Excel". Một số report khi bấm sẽ MỞ THÊM 1 TAB MỚI
    (popup) bên cạnh việc tải file — không phải report nào cũng vậy. Xử
    lý cả 2 trường hợp: có popup thì đóng lại trước khi tiếp tục; không
    có thì bỏ qua, không chờ vô ích quá lâu.

    Một số nút export nằm trong .tooltip-content (chỉ hiện khi hover vào
    phần tử cha) — nếu hover_selector được cung cấp, hover vào đó trước
    để tooltip kịp hiện ra, tránh lỗi "element not visible".

    Trả về: Download object (Playwright) để lưu file.
    """
    if hover_selector:
        # Panel tham số (.report-param) sau khi "Xem báo cáo" có thể còn mở và
        # ĐÈ lên toolbar export -> hover bị "intercepts pointer events". Thử
        # thu gọn panel trước (bấm nút collapse nếu có), rồi mới hover.
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

        print(f"    [export] đang hover vào '{hover_selector}'...")
        try:
            await page.locator(hover_selector).first.hover(timeout=20000)
            await page.wait_for_timeout(400)  # đợi tooltip-content kịp render
            print("    [export] hover OK.")
        except Exception as e:
            print(f"    [export] hover lỗi/bị che ({e}) — sẽ dùng dispatch_event.")

    target = page.locator(export_selector)

    # ── TRƯỜNG HỢP A: có DIALOG chọn cột (ITEM_LIST) ─────────────────────
    # Bấm export -> dialog hiện (CHƯA tải) -> tích cột -> bấm "Đồng ý" mới tải.
    # Tách bạch: expect_download CHỈ bọc quanh nút "Đồng ý", không bọc bước
    # mở dialog (nếu bọc cả, Playwright đứng chờ file trong khi dialog chưa xong).
    if confirm_dialog_button:
        import time as _t

        async def _dialog_open():
            # Dialog "Tùy chọn xuất ra Excel" — phát hiện qua tiêu đề hoặc
            # container dialog/popup đang hiện.
            for sel in ["text=Tùy chọn xuất ra Excel",
                        ".ms-dialog:visible", ".ms-modal:visible",
                        ".ms-popup:visible"]:
                try:
                    if await page.locator(sel).first.is_visible():
                        return True
                except Exception:
                    pass
            return False

        # Bấm export bằng CLICK THƯỜNG (force=True không trigger mở dialog).
        # Retry tối đa 3 lần cho tới khi dialog mở.
        for attempt in range(3):
            print(f"    [export] bấm export ('{export_selector}') mở dialog (lần {attempt+1})...")
            try:
                await (target.first if use_first else target).click(timeout=10_000)
            except Exception:
                try:
                    await (target.first if use_first else target).click(force=True, timeout=8_000)
                except Exception:
                    pass
            # chờ dialog mở tối đa 8s
            _d = _t.monotonic() + 8
            while _t.monotonic() < _d:
                if await _dialog_open():
                    break
                await page.wait_for_timeout(500)
            if await _dialog_open():
                print("    [export] dialog đã mở.")
                break
        else:
            print("    [export] CẢNH BÁO: dialog không mở sau 3 lần bấm.")

        # Tên nút xác nhận có thể gặp — bao phủ rộng
        names = [confirm_dialog_button, "Đồng ý", "Đồng Ý", "Áp dụng",
                 "Xác nhận", "Thực hiện", "Kết xuất", "Xuất khẩu", "Xuất",
                 "Tải về", "Tải xuống", "Hoàn thành", "Tiếp tục", "OK", "Có"]
        names = [n for n in names if n]

        async def _find_confirm_button():
            # 1) theo TÊN, trên nhiều loại element (button/div/span/a)
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
            # 2) theo KIỂU nút primary (nút xanh xác nhận) trong dialog/popup
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

        # Chờ dialog + tích cột (nếu có), tối đa 30s
        btn = None
        _dl = _t.monotonic() + 30
        while _t.monotonic() < _dl:
            btn, matched = await _find_confirm_button()
            if btn is not None:
                print(f"    [export] dialog hiện — nút xác nhận: {matched}")
                break
            await page.wait_for_timeout(1000)

        # Tích cột nếu dialog có (và có yêu cầu)
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

        # Bấm xác nhận (nếu tìm được) + chờ download. Nếu KHÔNG tìm được nút,
        # vẫn chờ download vì có thể bấm export đã tải thẳng (best-effort).
        print("    [export] chờ download...")
        # ── TRƯỜNG HỢP B: bấm export tải luôn (report khác) ──────────────────
        
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

    # ── TRƯỜNG HỢP B: bấm export tải luôn (report khác) ──────────────────
    print(f"    [export] đang bấm export ('{export_selector}'), chờ download...")
    async with page.expect_download(timeout=download_timeout) as download_info:
        clicked = False
        # Thử click bình thường (kèm phát hiện popup)
        try:
            async with page.expect_popup(timeout=10_000) as popup_info:
                await (target.first if use_first else target).click(
                    force=True, timeout=15_000
                )
            clicked = True
            popup_page = await popup_info.value
            await popup_page.close()
            print("    [export] có mở tab phụ — đã đóng lại.")
        except Exception:
            pass
        # Nếu click thường bị che/trượt -> dispatch_event xuyên qua lớp phủ
        if not clicked:
            try:
                await (target.first if use_first else target).dispatch_event("click")
                print("    [export] đã click bằng dispatch_event (xuyên lớp phủ).")
            except Exception as e:
                print(f"    [export] dispatch_event cũng lỗi: {e}")
    print("    [export] download xong.")
    return await download_info.value


async def is_session_expired(page) -> bool:
    """
    Kiểm tra theo NỘI DUNG trang, không chỉ URL — vì MISA dùng CHUNG 1 URL
    (Account/Join) cho cả trạng thái đã đăng nhập (hiện danh sách công ty)
    và chưa đăng nhập (hiện form đăng nhập), nên không thể phân biệt 2
    trạng thái này chỉ bằng URL. Có ô nhập mật khẩu -> chưa đăng nhập.
    """
    try:
        password_input = page.locator("input[type='password']")
        return await password_input.count() > 0
    except Exception:
        return False


async def click_company_card_button(page, name_hint: str, timeout: int = 10000) -> None:
    """
    Bấm nút "Truy cập" trong đúng card công ty (Cấp 1) khớp tên name_hint.
    Cấu trúc DOM thật (lấy từ Inspect):
      div.invite-company-ele > div.company-info > (span.name-company[title=...], button.access)
    Dùng .filter(has=...) để chắc chắn bấm đúng nút NẰM TRONG card có tên
    khớp — không dựa vào .first (rủi ro nếu sau này có nhiều công ty ở
    cấp này).
    """
    card = page.locator(".invite-company-ele").filter(
        has=page.locator(".name-company", has_text=name_hint)
    )
    await card.locator("button.access").click(timeout=timeout)


async def login_and_select_company(context):
    """
    Luồng đăng nhập + chọn công ty THẬT, ghi lại bằng Playwright codegen:

      1. Vào trang Account/Join.
      2. Bấm "Truy cập" ở CẤP 1 — đây là công ty CỦA KẾ TOÁN (vd công ty
         kiểm toán/dịch vụ kế toán quản lý nhiều khách hàng), KHÔNG phải
         công ty cần lấy báo cáo. Chỉ có 1 công ty ở cấp này nên dùng
         .first an toàn.
      3. (Có thể có) đóng banner quảng cáo nếu MISA hiện ra — không phải
         lúc nào cũng có, nên bọc try/except, bỏ qua nếu không thấy.
      4. Bấm vào tên công ty THẬT cần lấy dữ liệu ở CẤP 2 (danh sách công
         ty mà kế toán đó quản lý) — khớp theo COMPANY_NAME_HINT.
      5. Bấm nút hành động trên dòng vừa chọn -> MỞ TAB MỚI (popup), đây
         mới là app công ty thật, nơi report nằm.

    Trả về: page của TAB MỚI (popup) — dùng tab này cho mọi report phía
    sau, KHÔNG dùng lại page ban đầu (page ban đầu chỉ để chọn công ty).
    """
    page = await context.new_page()
    await page.goto(MISA_LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)
    
    # Chờ thêm để Vue app kịp render xong nội dung thật (tránh false positive
    # ở is_session_expired do domcontentloaded chỉ có HTML tĩnh ban đầu).
    try:
        await page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass  # không sao nếu vẫn còn request nền chạy ngầm liên tục

    if await is_session_expired(page):
        raise SystemExit(
            "Session hết hạn ngay từ đầu. "
            "Chạy lại: python misa_report_downloader_win.py --setup"
        )

    # Cấp 1: công ty của kế toán — bấm đúng theo tên, không dùng .first chung
    await click_company_card_button(page, ACCOUNTING_FIRM_NAME_HINT)

    # Banner quảng cáo — không cố định xuất hiện, bỏ qua nếu không thấy.
    try:
        await page.get_by_role("button").filter(has_text=re.compile(r"^$")).first.click(
            timeout=3000
        )
        print("    Đã đóng banner quảng cáo.")
    except Exception:
        pass

    try:
        await page.locator(".customer-name").first.wait_for(
            state="visible", timeout=60000
        )
    except Exception:
        pass  # nếu vẫn timeout thì để get_attribute thử tiếp, sẽ báo lỗi rõ hơn

    # Cấp 2: tìm đúng DÒNG công ty khớp tên, lấy id duy nhất của dòng đó
    # (mỗi <tr> có id riêng, vd "BQ1384WYT3HV"), rồi dùng id đó để bấm
    # CHÍNH XÁC nút "Xem dữ liệu" (data-customerid khớp id) — đáng tin hơn
    # hẳn so với .selected/.odd, vì không phụ thuộc thứ tự dòng hay việc
    # click tên trước có "chọn" đúng dòng hay không.
    
    # Sau:
    await page.locator(".customer-name").first.wait_for(state="visible", timeout=60000)
    customer_row = page.locator("tr").filter(
        has=page.locator(".customer-name", has_text=COMPANY_NAME_HINT)
    )
    customer_id = await customer_row.first.get_attribute("id", timeout=60000)  # ← tăng timeout

    if not customer_id:
        raise RuntimeError(
            f"Không tìm thấy dòng công ty khớp '{COMPANY_NAME_HINT}' để lấy "
            f"id — dừng lại, không bấm bừa vào công ty khác."
        )
    print(f"    Tìm thấy dòng công ty khớp, id={customer_id}.")

    # Bấm "Xem dữ liệu" đúng dòng -> mở tab mới (app công ty thật).
    # force=True vì MISA dùng DataTables FixedColumns (DTFC) — có bản sao
    # cột đè lên (đã gặp y hệt ở bước Cấp 1), chặn click bình thường.
    async with page.expect_popup() as popup_info:
        await page.locator(f'.il-text[data-customerid="{customer_id}"]').first.click(
            force=True, timeout=10000
        )
    company_page = await popup_info.value
    # Chờ URL redirect xong khỏi trang callback. Poll thủ công để chịu được
    # trường hợp MISA redirect nhiều chặng/chậm, thay vì wait_for_url cứng
    # (dễ timeout 60s khi có chặng trung gian không khớp điều kiện).
    import time as _t
    deadline = _t.monotonic() + 180  # tối đa 3 phút
    while _t.monotonic() < deadline:
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
    await company_page.wait_for_timeout(2000)  # đợi JS render tên công ty vào DOM

    print(f"    Đã chọn công ty, chuyển sang tab mới: {company_page.url}")

    # Xác minh lại — chọn nhầm công ty là rủi ro nghiêm trọng (lấy sai dữ
    # liệu mà không dễ nhận ra), nên kiểm tra thêm 1 lần.
    page_text = await company_page.content()
    if COMPANY_NAME_HINT.upper() not in page_text.upper():
        raise RuntimeError(
            f"Không xác nhận được đang ở đúng công ty '{COMPANY_NAME_HINT}' "
            f"sau khi chọn — DỪNG LẠI, không tiếp tục tải report để tránh lấy "
            f"nhầm dữ liệu công ty khác."
        )

    return company_page


async def download_reports(
    headless: bool = True,
    from_date_arg: str | None = None,
    to_date_arg: str | None = None,
    use_bookmark: bool = True,          # ← THÊM
) -> None:
    """Chạy tự động — dùng lại đúng profile đã đăng nhập ở login_setup()."""
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
    print(f"Khoảng ngày áp dụng cho mọi report cần tham số: {from_date} -> {to_date}")

    # Mỗi lần chạy tạo 1 folder riêng theo thời gian chạy, chứa toàn bộ
    # report tải được trong lần đó + 1 file log ghi lại mọi dòng in ra
    # terminal — dễ đối chiếu sau này, không bị lẫn giữa các lần chạy.
    # run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    # run_dir = INBOX_ROOT / run_id
    # run_dir.mkdir(parents=True, exist_ok=True)
    # log_path = run_dir / "run.log"
    # log_file = open(log_path, "w", encoding="utf-8")
    # original_stdout = sys.stdout
    # sys.stdout = Tee(original_stdout, log_file)
    # print(f"=== Bắt đầu lần chạy: {run_id} — log lưu tại {log_path} ===")

    errors: list[str] = []


    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=headless,
            # Chạy chậm lại (ms giữa mỗi hành động) khi --show, để mắt theo
            # kịp xem nó đang bấm vào đâu. Khi headless thật (chạy lịch tự
            # động) thì slow_mo=0, không làm chậm automation.
            slow_mo=0 if headless else 2000,
            # Mở full màn hình khi --show (viewport=None để trang tự co
            # giãn theo kích thước cửa sổ thật, thay vì khung cố định nhỏ).
            # Khi headless thì không có cửa sổ thật, dùng viewport cố định
            # rộng để table/report hiển thị đủ, không bị cắt.
            args=(["--start-maximized"] if not headless else []) + [
                "--disable-blink-features=AutomationControlled",
            ],
            viewport=None if not headless else {"width": 1600, "height": 900},
        )

        page = await login_and_select_company(context)

        # Đóng popup quảng cáo ngay sau khi vào dashboard
        await dismiss_message_overlay(page)
        await page.wait_for_timeout(500)

        for report_cfg in REPORTS_TO_DOWNLOAD:
            print(f"\n--- Đang xử lý: {report_cfg['report_code']} ---")
            try:

                # Đóng overlay trước khi navigate — tránh bị che khi chuyển report
                await dismiss_message_overlay(page)

                print("    [nav] đang điều hướng tới report...")
                await navigate_to_report(page, report_cfg)
                print(f"    [nav] đã vào: {page.url}")


                if await is_session_expired(page):
                    raise RuntimeError(
                        "Bị đẩy về trang đăng nhập — session đã hết hạn, "
                        "cần chạy lại --setup."
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

                # ── PHẢI CÓ if ở đây, không dính vào needs_deselect_all ──
                if report_cfg.get("needs_params"):
                    _today = date.today()
                    _fmt   = "%d/%m/%Y"

                    if report_cfg["report_code"] in ("B01_DN", "B02_DN"):
                        _fmt = "%d/%m/%Y"

                        if use_bookmark:
                            # Bookmark BẬT → tự động: đầu tháng hiện tại → hôm nay
                            _today = date.today()
                            _month_start = date(_today.year, _today.month, 1)
                            _from = _month_start.strftime(_fmt)
                            _to = _today.strftime(_fmt)
                            print(f"    [date-override][bookmark=ON] {report_cfg['report_code']}: {_from} → {_to}")
                        else:
                            # Bookmark TẮT → dùng đúng from_date/to_date người dùng đã chọn tay
                            _from = from_date
                            _to = to_date
                            print(f"    [date-override][bookmark=OFF] {report_cfg['report_code']}: dùng filter thủ công {_from} → {_to}")

                        await set_date_range(page, _from, _to,
                                            panel_already_open=report_cfg.get("date_panel_open", False))

                    elif report_cfg.get("force_from_year_start"):
                        year_start = date(_today.year, 1, 1).strftime(_fmt)
                        await set_date_range(page, year_start, to_date,
                                            panel_already_open=report_cfg.get("date_panel_open", False))
                    else:
                        await set_date_range(page, from_date, to_date,
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
                            await btn.first.wait_for(state="visible", timeout=15000)
                            await btn.first.click(timeout=5000)
                            await page.wait_for_timeout(1000)
                            print("    [select-all] đã bấm.")
                        except Exception as e:
                            print(f"    [select-all] lỗi text 'Chọn tất cả trên bảng': {e}")
                            try:
                                select_all = page.locator(".rp-grid-selector__select-all-group")
                                if await select_all.first.is_visible():
                                    await select_all.first.click()
                                    await page.wait_for_timeout(1000)
                                    print("    [select-all] đã bấm (fallback selector).")
                            except Exception as e2:
                                print(f"    [select-all] fallback cũng lỗi: {e2}")

                    if report_cfg.get("needs_item_selection"):
                        await select_all_items_in_table(page)

                # ── THÊM CHECK skip_false_button ──────────────────────────
                if not report_cfg.get("skip_false_button"):
                    await click_mystery_false_button(page)
                # ──────────────────────────────────────────────────────────

                if report_cfg.get("view_report_first"):
                    await click_view_report_button(page)

                await dismiss_message_overlay(page)

                export_selector = report_cfg.get("export_selector", EXPORT_BUTTON_SELECTOR)
                hover_selector = report_cfg.get("export_hover_selector")
                download = await click_export_and_download(
                    page, export_selector, hover_selector=hover_selector,
                    confirm_dialog_button=report_cfg.get("export_confirm_button"),
                    export_columns=report_cfg.get("export_columns"),
                )

                # Lưu trực tiếp vào run_dir, có tiền tố report_code để
                # không lẫn giữa các report khi không dùng subfolder.
                # dest_path = run_dir / f"{report_cfg['report_code']}__{download.suggested_filename}"
                # await download.save_as(dest_path)
                # print(f"[OK] {report_cfg['report_code']} -> {dest_path}")
                std_name = STANDARD_FILENAME.get(report_cfg["report_code"])
                if not std_name:
                    raise RuntimeError(f"Không có tên file chuẩn cho {report_cfg['report_code']}")
                MISA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                dest_path = MISA_OUTPUT_DIR / std_name
                if dest_path.exists():
                    dest_path.unlink()
                    print(f"    [save] đã xóa file cũ: {dest_path.name}")
                await download.save_as(dest_path)
                print(f"[OK] {report_cfg['report_code']} -> {dest_path}")

            except Exception as e:
                print(f"[ERROR] {report_cfg['report_code']}: {e}")
                errors.append(report_cfg["report_code"])

        await context.close()
    # finally:
    #     sys.stdout = original_stdout
    #     log_file.close()

    if errors:
        raise SystemExit(f"Hoàn tất với lỗi ở: {errors}")

    print(f"\nXong — đã tải toàn bộ {len(REPORTS_TO_DOWNLOAD)} report vào {MISA_OUTPUT_DIR}")

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
    # ── THÊM MỚI ──────────────────────────────────────────────────────────
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
        help="'true' = B01/B02 tự tính đầu tháng->hôm nay. 'false' = B01/B02 dùng đúng --from-date/--to-date được truyền.",
    )
    # ─────────────────────────────────────────────────────────────────────

    args = parser.parse_args()

    # ── FILTER REPORTS THEO --reports ─────────────────────────────────────
    if args.reports:
        selected = [r.strip() for r in args.reports.split(",") if r.strip()]
        REPORTS_TO_DOWNLOAD[:] = [
            r for r in REPORTS_TO_DOWNLOAD
            if r["report_code"] in selected
        ]
        print(f"Chỉ tải {len(REPORTS_TO_DOWNLOAD)} report: {selected}")
    # ─────────────────────────────────────────────────────────────────────

    if args.setup:
        asyncio.run(login_setup())
    else:
        asyncio.run( 
            download_reports(

                # headless=False,
                headless=not args.show,  # ← True khi không có --show, False khi có --show
                from_date_arg=args.from_date,
                to_date_arg=args.to_date,
                use_bookmark=(args.use_bookmark.strip().lower() == "true"),   # ← THÊM
            )
        )

        