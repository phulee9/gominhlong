"""Fact_BalanceSheet: Báo cáo tình hình tài chính B01-DN — quét Reporting_Date, chuẩn hoá mã chỉ tiêu."""
import re

import pandas as pd

from .base import BaseTransformer, TransformContext


class FactBalanceSheetTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        # 1. Quét tìm ngày tháng (Đã confirm chạy được)
        report_date = None
        try:
            df_head_temp = pd.read_excel(
                ctx.file_path, sheet_name=ctx.sheet, nrows=15, header=None, engine="openpyxl"
            )
            for _, r in df_head_temp.iterrows():
                for cell in r.values:
                    if "tại ngày" in str(cell).lower():
                        match = re.search(
                            r'tại\s+ngày\s+(\d{1,2})\s+tháng\s+(\d{1,2})\s+năm\s+(\d{4})',
                            str(cell), re.IGNORECASE
                        )
                        if match:
                            day, month, year = match.groups()
                            report_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                            break
                if report_date:
                    break
        except Exception:
            pass

        # 2. Xử lý làm sạch cột (Xóa Unnamed)
        df = df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]

        # 3. Định nghĩa hàm format và Map mã
        def format_b01_code(code):
            c = str(code).strip()
            # Xử lý trường hợp Pandas đọc nhầm số thành float (ví dụ: 100 -> 100.0)
            if c.endswith('.0'):
                c = c[:-2]
            return f"B01-DN_{c}" if (c != "nan" and c != "" and c != "None") else None

        df['Indicator_Code'] = df['Indicator_Code'].apply(format_b01_code)
        df = df.dropna(subset=['Indicator_Code'])

        # 4. Gán Reporting_Date (Gán sau cùng để đảm bảo tồn tại trong DataFrame)
        df['Reporting_Date'] = report_date

        # 5. Ép kiểu dữ liệu số
        for col in ['Beginning_Balance', 'Ending_Balance']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return df
