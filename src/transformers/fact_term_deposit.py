"""Fact_TermDeposit: Hợp đồng tiền gửi — dọn dòng trống, chuẩn hoá lãi suất, đánh ID."""
import pandas as pd

from .base import BaseTransformer, TransformContext


class FactTermDepositTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        # Dọn dẹp dòng trống
        df = df.dropna(subset=['Passbook_No'])

        # Xử lý lãi suất (bỏ dấu % nếu có)
        if 'Interest_Rate' in df.columns:
            df['Interest_Rate'] = df['Interest_Rate'].astype(str).str.replace('%', '').str.replace(',', '.').str.strip()
            df['Interest_Rate'] = pd.to_numeric(df['Interest_Rate'], errors='coerce')

        # Tạo ID tự tăng
        df = df.reset_index(drop=True)
        df['ID'] = df.index + 1
        return df
