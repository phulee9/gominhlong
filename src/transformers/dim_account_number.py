"""
Dim_AccountNumber: tra Bank_Code từ Bank_Name.

ĐÃ SỬA so với bản gốc:
  Code cũ đọc trực tiếp file "Danh_sach_ngan_hang.xlsx" nằm CÙNG THƯ MỤC LOCAL
  với file đang xử lý (os.path.dirname(file_path)). Cách này gãy ngay khi chạy
  qua MinIO/Airflow: load_task download file Excel về 1 thư mục /tmp riêng,
  không có file "hàng xóm" nào nằm cạnh -> lookup luôn None mà không báo lỗi rõ.

  Giờ tra cứu thẳng từ bảng Dim_Bank ĐÃ LOAD trong DB (file_id="dim_bank") qua
  ctx.lookup("Dim_Bank"), nội dung giống hệt dữ liệu nguồn, không phụ thuộc
  layout thư mục.

YÊU CẦU VẬN HÀNH: phải load file_id="dim_bank" trước khi load file_id=
"dim_account_number" (xem dependency trong DAG Airflow / thứ tự chạy CLI).
Nếu Dim_Bank chưa có dữ liệu, Bank_Code sẽ để trống và có log cảnh báo —
không làm pipeline fail, để không chặn các bảng không liên quan.
"""
import logging
import re
import unicodedata

import pandas as pd

from .base import BaseTransformer, TransformContext

logger = logging.getLogger(__name__)


def _normalize_bank_name(name) -> str:
    if pd.isna(name):
        return ""
    name = unicodedata.normalize("NFC", str(name)).lower()
    words_to_remove = ["ngân hàng", "tmcp", "tnhh", "mtv", "cổ phần", "thương mại", "đại chúng"]
    for w in words_to_remove:
        name = name.replace(w, " ")
    name = re.sub(r"[^\w\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


class DimAccountNumberTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        if ctx.lookup is None:
            logger.warning(
                "[Dim_AccountNumber] Không có kết nối DB để lookup Dim_Bank "
                "(dry-run?). Bank_Code sẽ để trống."
            )
            df["Bank_Code"] = None
            return df

        df_bank = ctx.lookup("Dim_Bank")
        if df_bank is None or df_bank.empty or "Bank_Code" not in df_bank.columns:
            logger.warning(
                "[Dim_AccountNumber] Bảng Dim_Bank chưa có dữ liệu hoặc thiếu cột "
                "Bank_Code. Hãy load file_id='dim_bank' trước. Bank_Code sẽ để trống."
            )
            df["Bank_Code"] = None
            return df

        df_bank = df_bank.dropna(subset=["Bank_Code"]).copy()
        df_bank["Normalized_Key"] = df_bank["Bank_Name"].apply(_normalize_bank_name)
        bank_mapping_dict = dict(zip(df_bank["Normalized_Key"], df_bank["Bank_Code"]))

        df["Normalized_Name"] = df["Bank_Name"].apply(_normalize_bank_name)
        df["Bank_Code"] = df["Normalized_Name"].map(bank_mapping_dict)
        df = df.drop(columns=["Normalized_Name"])
        return df
