"""Fact_IncomeStatement: Báo cáo KQKD B02-DN — quét Month từ tiêu đề, chuẩn hoá mã chỉ tiêu."""
import logging
import re

import pandas as pd

from .base import BaseTransformer, TransformContext

logger = logging.getLogger(__name__)


class FactIncomeStatementTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        # 1. Quét tìm "Tháng phát sinh" (Ngày chốt báo cáo) từ dòng tiêu đề
        report_date = None
        try:
            df_head_temp = pd.read_excel(
                ctx.file_path, sheet_name=ctx.sheet, nrows=10, header=None, engine="openpyxl"
            )
            for _, r in df_head_temp.iterrows():
                for cell in r.values:
                    cell_str = str(cell).lower()
                    # Tìm dòng "Kỳ kế toán từ ngày... đến ngày..."
                    if "kỳ kế toán" in cell_str or "từ ngày" in cell_str:
                        match = re.search(r'đến ngày\s+(\d{1,2}/\d{1,2}/\d{4})', cell_str)
                        if match:
                            report_date = match.group(1)
                            break
                if report_date:
                    break
        except Exception as e:
            logger.warning(f"Lỗi tìm ngày báo cáo B02: {e}")

        if report_date:
            df['Month'] = pd.to_datetime(report_date, format="%d/%m/%Y").date()
        else:
            df['Month'] = None

        # 2. DỌN RÁC: Chỉ giữ lại các dòng CÓ MÃ SỐ thật sự (01, 02, 10...)
        # Lệnh này sẽ tự động "chém" bay các dòng rác như "NGƯỜI LẬP", "Kế toán trưởng"
        df = df.dropna(subset=['Indicator_Code'])
        df = df[df['Indicator_Code'].astype(str).str.strip() != '']
        df = df[df['Indicator_Code'].astype(str).str.lower() != 'nan']

        # 3. CHUẨN HÓA MÃ: Ghép tiền tố "B02-DN_" để Khớp (JOIN) được với bảng Dim_ReportItem
        def format_b02_code(code):
            code_str = str(code).strip()
            # Xử lý lỗi Pandas đôi khi tự biến chuỗi '01' thành số float '1.0'
            if code_str.endswith('.0'):
                code_str = code_str[:-2]
            if len(code_str) == 1:
                code_str = '0' + code_str
            return f"B02-DN_{code_str}"

        df['Indicator_Code'] = df['Indicator_Code'].apply(format_b02_code)

        # 4. Xử lý giá trị Lũy kế (YTD)
        # Thường báo cáo B02 xuất từ đầu năm đến hiện tại thì "Kỳ này" chính là Lũy kế YTD
        df['YTD_Amount'] = df['Current_Period_Amount']

        return df
