"""Fact_CashFlow: Sổ chi tiết các tài khoản — bóc Account_No từ dòng "Tài khoản: ...", forward-fill, lọc rác."""
import re

import numpy as np
import pandas as pd

from .base import BaseTransformer, TransformContext


class FactCashFlowTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        def extract_account(val):
            val_str = str(val).strip()
            # Tìm dòng có chữ "Tài khoản:" (ví dụ: Tài khoản: 1111 - Tiền mặt)
            if val_str.lower().startswith("tài khoản"):
                match = re.search(r'tài khoản:\s*([a-zA-Z0-9_]+)', val_str.lower())
                if match:
                    return match.group(1).upper()
            return np.nan

        # 1. Trích xuất mã tài khoản từ cột Ngày hạch toán (Cột A chứa dòng tiêu đề nhóm)
        df['Account_No'] = df['Posting_Date'].apply(extract_account)

        # 2. Forward-fill (kéo thả dữ liệu): Bê mã tài khoản vừa bốc được rải xuống các dòng dưới
        df['Account_No'] = df['Account_No'].ffill()

        # 3. Lọc sạch rác: Xóa các dòng tiêu đề, dòng "Số dư đầu kỳ/Cuối kỳ", "Cộng phát sinh"
        # (Đặc điểm nhận dạng giao dịch thật: Phải có Số chứng từ hợp lệ)
        df = df.dropna(subset=['Voucher_No'])
        df = df[df['Voucher_No'].astype(str).str.strip() != '']
        df = df[df['Voucher_No'].astype(str).str.lower() != 'nan']

        # 4. Khởi tạo ID tự tăng
        df = df.reset_index(drop=True)
        df['ID'] = df.index + 1
        return df
