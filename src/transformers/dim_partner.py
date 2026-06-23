"""Dim_Partner: gộp danh mục Khách hàng + Nhà cung cấp vào cùng 1 bảng."""
from .base import BaseTransformer, TransformContext
import pandas as pd


class DimPartnerTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        if "khach_hang" in ctx.file_id:
            df["Partner_Group"] = "Khách hàng"
        elif "nha_cung_cap" in ctx.file_id:
            df["Partner_Group"] = "Nhà cung cấp"
        return df
