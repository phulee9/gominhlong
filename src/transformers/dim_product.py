"""Dim_Product: phân loại Product_Category dựa trên từ khóa trong tên sản phẩm."""
import pandas as pd

from .base import BaseTransformer, TransformContext


class DimProductTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        def categorize_product(product_name):
            if pd.isna(product_name):
                return "Khác"

            name_lower = str(product_name).lower()

            # Quy chuẩn gom nhóm dựa trên từ khóa đặc trưng sản phẩm của Minh Long
            if "mdf" in name_lower:
                return "Ván MDF"
            elif "hdf" in name_lower:
                return "Ván HDF"
            elif "acrylic" in name_lower:
                return "Tấm Acrylic"
            elif "bản lề" in name_lower or "nắp" in name_lower or "titus" in name_lower:
                return "Phụ kiện"
            elif "ván" in name_lower:
                return "Ván các loại"
            elif "nẹp" in name_lower or "chỉ" in name_lower:
                return "Nẹp chỉ cạnh"
            elif "chi phí" in name_lower or "dịch vụ" in name_lower:
                return "Dịch vụ / Chi phí"

            # Nếu không khớp bất kỳ quy chuẩn nào ở trên, xếp vào nhóm Khác
            return "Khác"

        df["Product_Category"] = df["Product_Name"].apply(categorize_product)
        return df
