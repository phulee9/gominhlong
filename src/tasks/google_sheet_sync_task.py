"""
google_sheet_sync_task.py
Tải 3 file từ Google Sheets về local, thay thế file Excel nội bộ cũ.
Dùng Google Drive export API (không cần parse sheet, tải nguyên file xlsx).

THÊM VÀO misa_download_dag.py:
  1. Import task này ở đầu file
  2. Thêm task sync_google_sheets chạy song song với run_misa_downloader
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import requests
import google.oauth2.service_account as service_account
import google.auth.transport.requests as _gtr
GRequest = _gtr.Request

logger = logging.getLogger(__name__)

# ── CẤU HÌNH ────────────────────────────────────────────────────────────────
CREDENTIALS_PATH = "/mnt/c/excel-pipeline/config/etl-gominhlong-4331d3a8cd93.json"

# Thư mục lưu file nội bộ (noibo)
NOIBO_DIR = Path("data/raw/noibo")

# Mapping: Google Sheet ID → tên file lưu local
GOOGLE_SHEETS = [
    {
        "name": "BC Tín dụng",
        "sheet_id": "1kzjtyFxmdvFmQ2ZFB01fyDbxw5VArTzR",
        "filename": "20260531_Minh_long_bc_tin_dung_2026.xlsx",
    },
    {
        "name": "Kế hoạch kinh doanh",
        "sheet_id": "1KLgpoVhLHJUbvCO9p-3v2-8AF0tCFCR2",
        "filename": "Ke_hoach_kinh_doanh_minh_long_2026.xlsx",
    },
    {
        "name": "Hợp đồng tiền gửi",
        "sheet_id": "1L15mC11mCtL30ElMX2EYKGaBZ1W5bV3H",
        "filename": "Hop_dong_tien_gui.xlsm",
    },
]

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
# ────────────────────────────────────────────────────────────────────────────


def _get_credentials():
    """Lấy credentials từ Service Account JSON."""
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH, scopes=SCOPES
    )
    # Refresh token nếu cần
    if not creds.valid:
        creds.refresh(Request())
    return creds

def _export_sheet_as_xlsx(sheet_id: str, creds) -> bytes:
    """
    Tải file trực tiếp từ Drive.
    File đã là .xlsx (upload thẳng lên Drive, không phải Google Sheets native)
    → dùng endpoint 'files.get?alt=media' để tải nguyên file,
      KHÔNG dùng '/export' (chỉ áp dụng cho Google Docs/Sheets native).
    """
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
    Tải cả 3 Google Sheet về local, ghi đè file cũ.
    Trả về dict tóm tắt kết quả.
    """
    import sys
    # Đảm bảo PROJECT_ROOT trong sys.path (giống upload_one/load_one)
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
    results = {}

    for sheet_cfg in GOOGLE_SHEETS:
        name      = sheet_cfg["name"]
        sheet_id  = sheet_cfg["sheet_id"]
        filename  = sheet_cfg["filename"]
        dest_path = NOIBO_DIR / filename

        try:
            logger.info(f"[google_sheet_sync] Đang tải: {name} ({sheet_id})...")
            content = _export_sheet_as_xlsx(sheet_id, creds)

            # Ghi đè file cũ
            dest_path.write_bytes(content)
            size_kb = len(content) // 1024
            logger.info(
                f"[google_sheet_sync] ✓ {name} → {dest_path} ({size_kb} KB)"
            )
            results[name] = {"status": "ok", "path": str(dest_path), "size_kb": size_kb}

        except Exception as e:
            logger.error(f"[google_sheet_sync] ✗ {name}: {e}")
            results[name] = {"status": "error", "error": str(e)}

    # Kiểm tra có lỗi nào không
    errors = [k for k, v in results.items() if v["status"] == "error"]
    if errors:
        raise RuntimeError(
            f"Sync Google Sheets thất bại cho: {errors}\n"
            f"Chi tiết: {results}"
        )

    logger.info(f"[google_sheet_sync] Hoàn tất — đã tải {len(GOOGLE_SHEETS)} file.")
    return results