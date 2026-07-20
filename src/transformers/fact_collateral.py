"""Fact_Collateral: Tài sản đảm bảo — bóc Bank_Code bằng forward-fill, chỉ giữ dòng tài sản thật."""
import re

import pandas as pd

from .base import BaseTransformer, TransformContext


class FactCollateralTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        df_raw = ctx.df_raw

        # MƯỢN LẠI 2 CỘT GỐC TỪ df_raw ĐỂ XỬ LÝ LOGIC
        # (Vì đã xóa khỏi config để DB không bị thừa cột)
        df['_Stt_Raw'] = df_raw.get('Stt', pd.Series(dtype=str))
        df['_Bank_Name_Or_Asset'] = df_raw.get('Tên tài sản', pd.Series(dtype=str))

        # 1. Xác định Bank_Code bằng Forward-fill
        def extract_bank_code(row):
            stt = str(row.get('_Stt_Raw', '')).strip()
            name = str(row.get('_Bank_Name_Or_Asset', '')).strip()
            if re.match(r'^[A-Z]$', stt):
                if 'CHƯA ĐƯA VÀO THẾ CHẤP' in name.upper():
                    return None
                return name.split()[0]
            return None

        df['Bank_Code_Extracted'] = df.apply(extract_bank_code, axis=1)
        df['Bank_Code'] = df['Bank_Code_Extracted'].ffill()

        # 2. Lọc chỉ giữ dòng tài sản (Stt_Raw là số)
        def is_asset_row(stt_raw):
            try:
                float(str(stt_raw).strip())
                return True
            except Exception:
                return False

        df = df[df['_Stt_Raw'].apply(is_asset_row)]

        # 3. Dọn dẹp cột tạm và reset ID
        df = df.drop(columns=['_Stt_Raw', '_Bank_Name_Or_Asset', 'Bank_Code_Extracted'], errors='ignore')
        df = df.dropna(subset=['Collateral_Type'])

        df = df.reset_index(drop=True)
        df['Row_ID'] = df.index + 1
        
        df["Bank_Code"] = df["Bank_Code"].str.upper()

        return df
