"""Fact_Inventory_Balance: Tổng hợp tồn kho — quét Snapshot_Date từ dòng tiêu đề,
lọc rác, đánh ID.

Cấu trúc file (đã xác nhận):
  Row 6: 'Từ ngày DD/MM/YYYY đến ngày DD/MM/YYYY'  ← lấy ngày ĐẾN NGÀY làm Snapshot_Date
  Row 8-9: header (multi-row)
  Row 10+: dữ liệu

Fix: dùng openpyxl đọc thẳng thay vì pandas để tránh lệch cột/dòng khi quét tiêu đề.
"""
import logging
import re
from datetime import date

import openpyxl
import pandas as pd

from .base import BaseTransformer, TransformContext

logger = logging.getLogger(__name__)

# Regex bắt ngày — 2 format MISA:
#   Format 1: "Từ ngày DD/MM/YYYY đến ngày DD/MM/YYYY" → lấy ngày đến ngày
#   Format 2: "Tháng M năm YYYY"                       → lấy ngày cuối tháng
_PAT_DEN_NGAY = re.compile(r'đến ngày\s+(\d{1,2}/\d{1,2}/\d{4})', re.IGNORECASE)
_PAT_THANG    = re.compile(r'tháng\s+(\d{1,2})\s+năm\s+(\d{4})', re.IGNORECASE)


def _last_day_of_month(year: int, month: int) -> date:
    """Trả về ngày cuối tháng."""
    import calendar
    return date(year, month, calendar.monthrange(year, month)[1])


def _extract_snapshot_date(file_path: str, sheet) -> date:
    """Đọc 10 dòng đầu bằng openpyxl, quét tìm Snapshot_Date theo 2 format."""
    try:
        wb = openpyxl.load_workbook(file_path, data_only=True, read_only=True)
        ws = wb[sheet] if isinstance(sheet, str) else list(wb.worksheets)[sheet]

        for row in ws.iter_rows(max_row=10, values_only=True):
            for cell in row:
                if cell is None:
                    continue
                cell_str = str(cell).strip()

                # Format 1: "đến ngày DD/MM/YYYY" → lấy ngày đó
                m = _PAT_DEN_NGAY.search(cell_str)
                if m:
                    try:
                        d = pd.to_datetime(m.group(1), format="%d/%m/%Y").date()
                        logger.info(f"[fact_inventory_balance] Snapshot_Date = {d} (format: đến ngày)")
                        wb.close()
                        return d
                    except ValueError:
                        pass

                # Format 2: "Tháng M năm YYYY" → lấy ngày cuối tháng đó
                m = _PAT_THANG.search(cell_str)
                if m:
                    try:
                        month, year = int(m.group(1)), int(m.group(2))
                        d = _last_day_of_month(year, month)
                        logger.info(f"[fact_inventory_balance] Snapshot_Date = {d} (format: Tháng {month} năm {year})")
                        wb.close()
                        return d
                    except (ValueError, OverflowError):
                        pass

        wb.close()
        logger.warning(
            "[fact_inventory_balance] Không tìm thấy ngày trong 10 dòng đầu "
            "— dùng ngày hôm nay làm fallback."
        )
    except Exception as e:
        logger.warning(f"[fact_inventory_balance] Lỗi khi quét Snapshot_Date: {e}")
    return date.today()


class FactInventoryBalanceTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:

        # 1. Quét Snapshot_Date từ tiêu đề file (dùng openpyxl, không dùng pandas)
        snapshot_date = _extract_snapshot_date(ctx.file_path, ctx.sheet)
        df['Snapshot_Date'] = snapshot_date

        # 2. Lọc dòng rác (thiếu Warehouse_Code hoặc Product_Code)
        df = df.dropna(subset=['Warehouse_Code', 'Product_Code'])
        df['Warehouse_Code'] = df['Warehouse_Code'].astype(str).str.strip()
        df['Product_Code']   = df['Product_Code'].astype(str).str.strip()
        df = df[df['Warehouse_Code'] != '']
        df = df[df['Product_Code'] != '']

        # 3. Đánh ID tự tăng
        df = df.reset_index(drop=True)
        df['ID'] = df.index + 1

        return df