"""Fact_IncomeStatement: Báo cáo KQKD B02-DN — quét Month từ tiêu đề, chuẩn hoá mã chỉ tiêu.

THAY ĐỔI THEO ISSUE DA (lũy kế):
  Báo cáo được export THEO TỪNG THÁNG (T1, T2, T3... mỗi tháng 1 file) nên
  1 file KHÔNG đủ dữ liệu để tự tính lũy kế. Bản cũ gán
  YTD_Amount = Current_Period_Amount là SAI.
  => BỎ cột YTD_Amount. THAY bằng Previous_Period_Amount: load 1:1 từ cột
  "Kỳ trước" có sẵn trong báo cáo (B02 luôn có cặp cột Kỳ này / Kỳ trước,
  tương tự B01 có Số cuối kỳ / Số đầu kỳ).
  Lũy kế nếu BI cần thì tính ở tầng BI bằng SUM(Current_Period_Amount)
  các tháng <= tháng đang xem — không tính ở ETL.

  YÊU CẦU KÈM THEO (ngoài file này):
  1. pipeline_config.yaml — field_mapping phải map cột "Kỳ trước" của file
     nguồn -> target "Previous_Period_Amount" (transformer sẽ raise lỗi rõ
     ràng nếu thiếu, không im lặng bỏ qua).
  2. DB: ALTER TABLE Fact_IncomeStatement — bỏ/ngưng dùng YTD_Amount,
     thêm Previous_Period_Amount DECIMAL. Report/measure BI nào đang đọc
     YTD_Amount phải sửa theo.
"""
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
                    # (b) "...đến ngày dd/mm/yyyy" — ngày cụ thể
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
                    # (a) "Kỳ kế toán tháng X năm Y" — quy về ngày cuối tháng
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

        # 4. Kỳ trước: load 1:1 từ cột "Kỳ trước" của báo cáo (issue DA).
        #    KHÔNG tự tính lũy kế ở ETL — file export theo từng tháng, 1 file
        #    không đủ dữ liệu; lũy kế tính ở tầng BI từ Current_Period_Amount.
        if 'Previous_Period_Amount' not in df.columns:
            raise ValueError(
                "fact_income_statement: thiếu cột 'Previous_Period_Amount'. "
                "Phải bổ sung field_mapping trong pipeline_config.yaml: "
                "cột 'Kỳ trước' của báo cáo B02 -> 'Previous_Period_Amount' "
                "(load 1:1, thay thế cột YTD_Amount cũ theo issue DA)."
            )

        # 5. Ép kiểu số cho 2 cột giá trị
        for col in ('Current_Period_Amount', 'Previous_Period_Amount'):
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Đảm bảo không còn YTD_Amount lọt xuống DB (nếu config cũ vẫn map)
        df = df.drop(columns=['YTD_Amount'], errors='ignore')

        logger.info(
            f"[B02][OUT] {len(df)} dòng | Month={report_date} | "
            f"Previous_Period_Amount null: {int(df['Previous_Period_Amount'].isna().sum())}"
        )
        return df