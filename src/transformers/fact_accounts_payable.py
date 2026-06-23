"""Fact_AccountsPayable: Công nợ phải trả — lọc dòng tổng cộng/không có chứng từ, đánh ID, ép kiểu số."""
import pandas as pd

from .base import BaseTransformer, TransformContext


class FactAccountsPayableTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        # Xóa dòng rác (dòng tổng cộng, dòng tên nhà cung cấp không có chứng từ)
        df = df.dropna(subset=['Partner_Code', 'Voucher_No'])

        # Tạo ID tự tăng
        df = df.reset_index(drop=True)
        df['ID'] = df.index + 1

        # Ép kiểu số cho các cột tiền tệ
        money_cols = ['Debit_Amount', 'Credit_Amount', 'Ending_Debit_Balance', 'Ending_Credit_Balance']
        for col in money_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return df
