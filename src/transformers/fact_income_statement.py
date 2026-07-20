"""Fact_IncomeStatement: Báo cáo KQKD B02-DN — quét Month từ tiêu đề, chuẩn hoá mã chỉ tiêu."""
import calendar
import logging
import re
import pandas as pd
from .base import BaseTransformer, TransformContext
logger = logging.getLogger(__name__)


class FactIncomeStatementTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        # 1. Quét tìm ngày kỳ báo cáo từ dòng tiêu đề.
        #    MISA có thể xuất 2 dạng khác nhau tùy khoảng lọc khi export:
        #      (a) Lọc TRÒN THÁNG  -> "Kỳ kế toán tháng 6 năm 2026"
        #                              (không có ngày cụ thể)
        #                              => Month = NGÀY CUỐI của tháng đó
        #      (b) Lọc GIỮA THÁNG  -> "...đến ngày 15/06/2026"
        #                              (có ngày cụ thể, dạng dd/mm/yyyy)
        #                              => Month = ĐÚNG ngày "đến ngày" (to_date)
        #    Thử pattern (b) trước vì nó CHÍNH XÁC hơn (có ngày thật), chỉ
        #    fallback sang pattern (a) khi không tìm thấy ngày cụ thể nào.
        report_date = None
        matched_pattern = None
        try:
            df_head_temp = pd.read_excel(
                ctx.file_path, sheet_name=ctx.sheet, nrows=15, header=None, engine="openpyxl"
            )
            for _, r in df_head_temp.iterrows():
                for cell in r.values:
                    cell_str = str(cell)
                    cell_lower = cell_str.lower()

                    # (b) Pattern CŨ: "...đến ngày dd/mm/yyyy" — ngày cụ thể
                    #     => dùng ĐÚNG ngày này làm Month, không quy về đâu cả
                    if report_date is None and (
                        "kỳ kế toán" in cell_lower or "từ ngày" in cell_lower
                    ):
                        match = re.search(
                            r'đến ngày\s+(\d{1,2}/\d{1,2}/\d{4})', cell_lower
                        )
                        if match:
                            report_date = pd.to_datetime(
                                match.group(1), format="%d/%m/%Y"
                            ).date()
                            matched_pattern = "partial-period (dùng đúng to_date)"
                            break

                    # (a) Pattern MỚI: "Kỳ kế toán tháng X năm Y" — chỉ tháng/năm
                    #     => quy về NGÀY CUỐI của tháng đó (không phải ngày 01)
                    if report_date is None and "kỳ kế toán" in cell_lower:
                        match = re.search(
                            r'tháng\s+(\d{1,2})\s+năm\s+(\d{4})',
                            cell_str, re.IGNORECASE
                        )
                        if match:
                            month, year = match.groups()
                            month, year = int(month), int(year)
                            last_day = calendar.monthrange(year, month)[1]
                            report_date = pd.Timestamp(
                                year=year, month=month, day=last_day
                            ).date()
                            matched_pattern = f"full-month (quy về ngày cuối tháng: {last_day})"
                            break

                if report_date:
                    break
        except Exception as e:
            logger.warning(f"Lỗi tìm ngày báo cáo B02: {e}")

        if report_date:
            df['Month'] = report_date
            logger.info(f"[B02] Đã xác định kỳ báo cáo: {report_date} (pattern: {matched_pattern})")
        else:
            logger.error("[B02] KHÔNG tìm được kỳ báo cáo (thử cả 2 pattern) — Month sẽ để trống")
            df['Month'] = None

        # 2. DỌN RÁC: Chỉ giữ lại các dòng CÓ MÃ SỐ thật sự (01, 02, 10...)
        df = df.dropna(subset=['Indicator_Code'])
        df = df[df['Indicator_Code'].astype(str).str.strip() != '']
        df = df[df['Indicator_Code'].astype(str).str.lower() != 'nan']

        # 3. CHUẨN HÓA MÃ
        def format_b02_code(code):
            code_str = str(code).strip()
            if code_str.endswith('.0'):
                code_str = code_str[:-2]
            if len(code_str) == 1:
                code_str = '0' + code_str
            return f"B02-DN_{code_str}"

        df['Indicator_Code'] = df['Indicator_Code'].apply(format_b02_code)

        # 4. Xử lý giá trị Lũy kế (YTD)
        df['YTD_Amount'] = df['Current_Period_Amount']

        return df