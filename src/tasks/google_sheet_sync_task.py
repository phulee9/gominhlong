"""
google_sheet_sync_task.py
Tải các file báo cáo từ Google Sheets về local, thay thế file Excel nội bộ cũ.

Danh sách link đọc từ 1 Google Sheet "bảng điều khiển" (CONTROL_SHEET_ID),
khách hàng chỉ có 2 cột, và CHỈ ĐƯỢC SỬA CỘT URL:

    | Tên báo cáo         | URL                                          |
    |----------------------|------------------------------------------------|
    | BC Tín dụng          | https://docs.google.com/.../d/1kzjt.../edit    |
    | Kế hoạch kinh doanh  | https://docs.google.com/.../d/1KLgp.../edit    |
    | Hợp đồng tiền gửi    | https://docs.google.com/.../d/1L15m.../edit    |

QUAN TRỌNG — khớp theo VỊ TRÍ DÒNG CỐ ĐỊNH, không khớp theo tên:
  Dòng dữ liệu thứ 1 (ngay sau header) → LUÔN là BC Tín dụng
  Dòng dữ liệu thứ 2                  → LUÔN là Kế hoạch kinh doanh
  Dòng dữ liệu thứ 3                  → LUÔN là Hợp đồng tiền gửi
Cột "Tên báo cáo" chỉ để khách hàng dễ nhìn/đối chiếu, KHÔNG dùng để match
trong code. Nếu khách lỡ xóa/chèn/đảo dòng, code KHÔNG detect được việc
này — sẽ tải nhầm dữ liệu vào nhầm file. Đây là đánh đổi đã chọn để tối
giản cho khách, đổi lấy rủi ro nếu khách thao tác sai ngoài phạm vi
"chỉ sửa URL".

TƯƠNG THÍCH NGƯỢC VỚI misa_download_dag.py (KHÔNG SỬA FILE DAG):
  DAG hiện tại có đoạn code tham chiếu trực tiếp
      _gg.GOOGLE_SHEETS  (đọc/gán/khôi phục biến này để lọc theo tên đã
      tick trong Airflow UI: gg_bc_tin_dung, gg_ke_hoach_kd, ...)
  Biến GOOGLE_SHEETS bên dưới được giữ lại CHỈ để đoạn code đó trong DAG
  không bị lỗi AttributeError khi chạy — nó KHÔNG còn ảnh hưởng thật đến
  việc tải file nữa (run() luôn đọc bảng điều khiển và tải đủ theo
  REPORT_ORDER, bất kể DAG có "lọc" biến này thế nào). Nói cách khác:
  TỪ NAY, việc tick/bỏ tick từng checkbox "gg_bc_tin_dung" v.v. trong
  Airflow UI SẼ KHÔNG còn tác dụng chọn lọc — mỗi lần chạy luôn tải đủ cả
  3 báo cáo theo đúng thứ tự cố định. Nếu muốn khôi phục khả năng chọn
  lọc từng báo cáo, cần sửa thêm DAG (ngoài phạm vi file này).
"""
from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# ── CẤU HÌNH ────────────────────────────────────────────────────────────────
CREDENTIALS_PATH = "/mnt/c/excel-pipeline/config/etl-gominhlong-4331d3a8cd93.json"

NOIBO_DIR = Path("data/raw/noibo")

# ID của Google Sheet "BẢNG ĐIỀU KHIỂN" — khách hàng chỉ sửa cột URL ở đây.
CONTROL_SHEET_ID = "1N8NTUEcutZJl_xwgXbn6dvvRlT1ix2XA2NaGNlri0rM"
CONTROL_SHEET_TAB_NAME = "Sheet1"

# Danh sách CỐ ĐỊNH theo ĐÚNG THỨ TỰ DÒNG trong bảng điều khiển.
REPORT_ORDER: list[dict] = [
    {"display_name": "BC Tín dụng",         "filename": "20260531_Minh_long_bc_tin_dung_2026.xlsx"},
    {"display_name": "Kế hoạch kinh doanh", "filename": "Ke_hoach_kinh_doanh_minh_long_2026.xlsx"},
    {"display_name": "Hợp đồng tiền gửi",   "filename": "Hop_dong_tien_gui.xlsm"},
]

# [TƯƠNG THÍCH NGƯỢC — xem docstring đầu file] Chỉ để DAG cũ không lỗi
# AttributeError khi đọc/gán/khôi phục biến này — KHÔNG còn tác dụng lọc
# thật sự với logic tải file (xem run() bên dưới).
GOOGLE_SHEETS: list[dict] = [{"name": r["display_name"]} for r in REPORT_ORDER]

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

# Bóc sheet_id/file_id từ mọi dạng URL Google Sheets/Drive phổ biến.
_ID_PATTERN = re.compile(r"/d/([a-zA-Z0-9_-]+)")
# ────────────────────────────────────────────────────────────────────────────


def _get_credentials():
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request

    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH, scopes=SCOPES
    )
    if not creds.valid:
        creds.refresh(Request())
    return creds


def extract_sheet_id(url: str) -> str:
    """Bóc sheet_id/file_id từ URL Google Sheets/Drive. Raise nếu không
    khớp được — báo lỗi rõ ràng thay vì âm thầm tải nhầm."""
    url = (url or "").strip()
    match = _ID_PATTERN.search(url)
    if not match:
        raise ValueError(
            f"Không bóc được ID từ URL: '{url}'. "
            f"URL cần chứa dạng '/d/<ID>/' (link chia sẻ chuẩn của Google Sheets/Drive)."
        )
    return match.group(1)


def _read_control_sheet_urls(creds) -> list[str]:
    """Đọc bảng điều khiển, trả về danh sách URL theo ĐÚNG THỨ TỰ DÒNG
    (giữ nguyên vị trí kể cả ô rỗng, để không lệch index so với REPORT_ORDER)."""
    from google.auth.transport.requests import Request

    auth_req = Request()
    creds.refresh(auth_req)
    headers = {"Authorization": f"Bearer {creds.token}"}

    api_url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{CONTROL_SHEET_ID}/values/"
        f"{CONTROL_SHEET_TAB_NAME}"
    )
    resp = requests.get(api_url, headers=headers, timeout=60)

    if resp.status_code == 403:
        raise PermissionError(
            f"Bảng điều khiển {CONTROL_SHEET_ID}: 403 Forbidden — chưa share "
            f"cho service account. Vào Google Sheet → Share → thêm email trong "
            f"file credentials.json → Viewer."
        )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Bảng điều khiển {CONTROL_SHEET_ID}: HTTP {resp.status_code} — {resp.text[:300]}"
        )

    values = resp.json().get("values", [])
    if not values:
        raise RuntimeError(f"Bảng điều khiển {CONTROL_SHEET_ID} rỗng.")

    header = [h.strip().lower() for h in values[0]]
    try:
        idx_url = header.index("url")
    except ValueError as e:
        raise RuntimeError(
            f"Bảng điều khiển thiếu cột 'URL'. Header đọc được: {values[0]}. Lỗi: {e}"
        )

    data_rows = values[1:]
    urls: list[str] = []
    for r in data_rows:
        url = r[idx_url].strip() if len(r) > idx_url else ""
        urls.append(url)

    return urls


def _export_sheet_as_xlsx(sheet_id: str, creds) -> bytes:
    """Tải file trực tiếp từ Drive (file .xlsx/.xlsm gốc, KHÔNG phải Google
    Sheets native) qua endpoint 'files.get?alt=media'."""
    from google.auth.transport.requests import Request

    url = f"https://www.googleapis.com/drive/v3/files/{sheet_id}"
    params = {"alt": "media"}

    auth_req = Request()
    creds.refresh(auth_req)
    headers = {"Authorization": f"Bearer {creds.token}"}

    resp = requests.get(url, params=params, headers=headers, timeout=120)

    if resp.status_code == 403:
        raise PermissionError(
            f"Sheet {sheet_id}: 403 Forbidden — chưa share cho service account. "
            f"Vào Google Sheet → Share → thêm email trong file credentials.json → Viewer."
        )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Sheet {sheet_id}: HTTP {resp.status_code} — {resp.text[:200]}"
        )
    return resp.content


def run(**context) -> dict:
    """
    Task function — gọi từ Airflow @task decorator.
    LUÔN đọc đủ URL theo ĐÚNG THỨ TỰ DÒNG trong bảng điều khiển, khớp 1-1
    với REPORT_ORDER, tải cả 3 file về local, ghi đè file cũ. Không còn
    hỗ trợ lọc theo từng report (xem ghi chú tương thích ngược ở đầu file).
    """
    project_root = "/mnt/c/excel-pipeline"
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    import os
    os.chdir(project_root)

    NOIBO_DIR.mkdir(parents=True, exist_ok=True)

    if not Path(CREDENTIALS_PATH).exists():
        raise FileNotFoundError(
            f"Không tìm thấy file credentials: {CREDENTIALS_PATH}\n"
            f"Làm theo hướng dẫn setup Google Service Account."
        )

    creds = _get_credentials()

    logger.info(f"[google_sheet_sync] Đang đọc bảng điều khiển ({CONTROL_SHEET_ID})...")
    urls = _read_control_sheet_urls(creds)
    logger.info(f"[google_sheet_sync] Đọc được {len(urls)} dòng dữ liệu từ bảng điều khiển.")

    if len(urls) < len(REPORT_ORDER):
        raise RuntimeError(
            f"Bảng điều khiển chỉ có {len(urls)} dòng, nhưng cần đủ "
            f"{len(REPORT_ORDER)} dòng (theo đúng thứ tự: "
            f"{[r['display_name'] for r in REPORT_ORDER]}). "
            f"Kiểm tra xem có dòng nào bị xóa nhầm không."
        )
    if len(urls) > len(REPORT_ORDER):
        logger.warning(
            f"[google_sheet_sync] Bảng điều khiển có {len(urls)} dòng, nhiều hơn "
            f"{len(REPORT_ORDER)} report đã cấu hình — các dòng thừa phía sau sẽ bị bỏ qua."
        )

    results = {}

    for i, report_cfg in enumerate(REPORT_ORDER):
        name = report_cfg["display_name"]
        filename = report_cfg["filename"]
        url = urls[i]
        dest_path = NOIBO_DIR / filename

        if not url:
            logger.error(
                f"[google_sheet_sync] ✗ Dòng {i+1} ('{name}' theo thứ tự cố định): "
                f"ô URL đang TRỐNG."
            )
            results[name] = {"status": "error", "error": f"Dòng {i+1} thiếu URL."}
            continue

        try:
            sheet_id = extract_sheet_id(url)
            logger.info(f"[google_sheet_sync] Đang tải: {name} (dòng {i+1}, id={sheet_id})...")
            content = _export_sheet_as_xlsx(sheet_id, creds)

            dest_path.write_bytes(content)
            size_kb = len(content) // 1024
            logger.info(
                f"[google_sheet_sync] ✓ {name} → {dest_path} ({size_kb} KB)"
            )
            results[name] = {
                "status": "ok", "path": str(dest_path),
                "size_kb": size_kb, "sheet_id": sheet_id,
            }

        except Exception as e:
            logger.error(f"[google_sheet_sync] ✗ {name} (dòng {i+1}): {e}")
            results[name] = {"status": "error", "error": str(e)}

    errors = [k for k, v in results.items() if v["status"] == "error"]
    if errors:
        raise RuntimeError(
            f"Sync Google Sheets thất bại cho: {errors}\n"
            f"Chi tiết: {results}"
        )

    logger.info(f"[google_sheet_sync] Hoàn tất — đã tải {len(REPORT_ORDER)} file.")
    return results