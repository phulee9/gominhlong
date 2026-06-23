"""Dim_Account: tách Account_BANK từ số/tên tài khoản 1121/1122 (TK ngân hàng)."""
import re

from .base import BaseTransformer, TransformContext
import pandas as pd


class DimAccountTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        def extract_bank_account(row):
            acc_no = str(row.get("Account_No", "")).strip()
            acc_name = str(row.get("Account_Name", ""))

            # Chỉ xử lý tách số nếu Số tài khoản bắt đầu bằng 1121 hoặc 1122
            if acc_no.startswith("1121") or acc_no.startswith("1122"):
                # Tìm chuỗi số liên tục (thường đứng sau chữ TK)
                match = re.search(r'\d+', acc_name)
                if match:
                    return match.group(0)
            return None

        df["Account_BANK"] = df.apply(extract_bank_account, axis=1)
        return df
