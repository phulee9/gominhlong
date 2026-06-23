"""Fact_InventoryOutward: Sổ chi tiết bán hàng — lọc dòng tổng/số dư, đánh ID."""
import pandas as pd

from .base import BaseTransformer, TransformContext


class FactInventoryOutwardTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        # Lọc bỏ các dòng "Cộng phát sinh", "Số dư" của file MISA
        # (Đặc điểm nhận dạng: Dòng phát sinh thật bắt buộc phải có Số chứng từ và Mã hàng)
        df = df.dropna(subset=['Voucher_No', 'Product_Code'])

        # Khởi tạo ID theo số thứ tự tăng dần
        df = df.reset_index(drop=True)
        df['ID'] = df.index + 1
        return df
