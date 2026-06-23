"""Fact_Loan: Báo cáo tín dụng — chỉ giữ cột chuẩn, loại dòng tổng cộng/trống, đánh ID."""
import pandas as pd

from .base import BaseTransformer, TransformContext


class FactLoanTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        # 1. Chỉ giữ cột A-K (index 0-10), bỏ các cột tháng về sau
        cols_to_keep = [c for c in df.columns if c in [
            'Bank_Code', 'Contract_No', 'Total_Credit_Limit',
            'Remaining_Principal', 'Interest_Rate',
            'Disbursement_Date', 'Maturity_Date', 'Principal_Payment_Amount'
        ]]
        df = df[cols_to_keep]

        # 2. Loại bỏ dòng rác:
        #    - Dòng tổng cộng: "Cộng vay NH", "Cộng vay MB"...
        #    - Dòng trống không có Contract_No
        df = df[df['Contract_No'].notna()]
        df = df[~df['Contract_No'].astype(str).str.lower()
                .str.contains('cộng|tổng|total', na=False)]
        df = df[df['Contract_No'].astype(str).str.strip() != '']
        df = df[df['Contract_No'].astype(str).str.lower() != 'nan']

        # 3. ID tự tăng
        df = df.reset_index(drop=True)
        df['ID'] = df.index + 1
        return df
