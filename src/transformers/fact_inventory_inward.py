"""Fact_InventoryInward: Sổ chi tiết mua hàng — lọc dòng tổng/số dư, đánh ID."""
import pandas as pd

from .base import BaseTransformer, TransformContext


class FactInventoryInwardTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        # Xóa các dòng tổng cộng/cộng phát sinh của MISA (do không có mã chứng từ và mã hàng thực tế)
        df = df.dropna(subset=['Voucher_No', 'Product_Code'])

        # Khởi tạo khóa chính ID (Số thứ tự tăng dần)
        df = df.reset_index(drop=True)
        df['ID'] = df.index + 1
        return df
