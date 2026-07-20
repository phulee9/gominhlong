"""
Dim_Product: Danh mục sản phẩm/hàng hóa + phân loại Product_Category.

NGUỒN FILE: Trang danh mục /app/DI/DIInventoryItems — file danh mục thuần túy.

Cấu trúc file (đã xác nhận):
  Row 1: "DANH SÁCH HÀNG HÓA, DỊCH VỤ"
  Row 2-3: trống
  Row 4: STT | Mã | Tên | Đơn vị tính chính  ← header
  Row 5+: dữ liệu thật
"""
import logging
import pandas as pd
from .base import BaseTransformer, TransformContext

logger = logging.getLogger(__name__)


def _categorize_product(product_name: str) -> str:
    if pd.isna(product_name):
        return "Khác"
    name_lower = str(product_name).lower()
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
    return "Khác"


class DimProductTransformer(BaseTransformer):

    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        df = df.reset_index(drop=True)
        initial_rows = len(df)

        # 1. Chuẩn hóa Product_Code
        df["Product_Code"] = df["Product_Code"].astype(str).str.strip()

        # 2. Loại dòng rỗng / không hợp lệ
        df = df[df["Product_Code"].notna()]
        df = df[~df["Product_Code"].isin(["", "None", "nan"])]

        # 3. Loại dòng tổng cuối file (nếu có)
        df = df[~df["Product_Code"].str.lower().str.contains(
            "tổng cộng|tong cong", na=False
        )]

        # 4. Dedup theo Product_Code — ưu tiên dòng CÓ Unit_of_Measure
        df["_has_uom"] = df["Unit_of_Measure"].isna()
        df = df.sort_values("_has_uom").drop(columns=["_has_uom"])
        df = df.drop_duplicates(subset=["Product_Code"], keep="first")
        df = df.reset_index(drop=True)

        # 5. Phân loại Product_Category
        df["Product_Category"] = df["Product_Name"].apply(_categorize_product)

        logger.info(
            "[dim_product] %d dòng → sau lọc & dedup: %d sản phẩm unique",
            initial_rows, len(df),
        )
        return df