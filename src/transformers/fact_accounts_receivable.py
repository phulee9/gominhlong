"""Fact_AccountsReceivable: Công nợ phải thu — lọc dòng tổng cộng, đánh ID, ép kiểu số."""
import pandas as pd

from .base import BaseTransformer, TransformContext


class FactAccountsReceivableTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        # 1. Xóa các dòng rác (thường là dòng "Tổng cộng" ở cuối file MISA)
        # Giả sử cột 'Partner_Code' rỗng thì đó là dòng tổng cộng
        df = df.dropna(subset=['Partner_Code'])

        # 2. Tạo cột ID tự tăng
        df = df.reset_index(drop=True)
        df['ID'] = df.index + 1

        # 3. Đảm bảo các cột tiền tệ là số (ép kiểu)
        money_cols = ['Debit_Amount', 'Credit_Amount', 'Ending_Debit_Balance', 'Ending_Credit_Balance']
        for col in money_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return df
