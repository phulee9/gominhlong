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

BUG ĐÃ FIX (gây Bank_Code = NULL toàn bộ):
  PostgreSQL tự động lowercase tên cột khi tạo bảng → ctx.lookup("Dim_Bank")
  trả về DataFrame với cột "bank_code", "bank_name" (chữ thường), KHÔNG PHẢI
  "Bank_Code", "Bank_Name" (PascalCase).
  Code cũ check `"Bank_Code" not in df_bank.columns` → luôn True → return None.
  Fix: lowercase toàn bộ tên cột ngay sau khi fetch, rồi tra cứu bằng tên thường.

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

# Tên cột trong DB sau khi PostgreSQL lowercase
_DB_CODE_COL = "bank_code"
_DB_NAME_COL = "bank_name"


def _normalize_bank_name(name) -> str:
    if pd.isna(name):
        return ""
    name = unicodedata.normalize("NFC", str(name)).lower()
    for w in ["ngân hàng", "tmcp", "tnhh", "mtv", "cổ phần", "thương mại", "đại chúng"]:
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

        if df_bank is None or df_bank.empty:
            logger.warning(
                "[Dim_AccountNumber] Bảng Dim_Bank chưa có dữ liệu. "
                "Hãy load file_id='dim_bank' trước. Bank_Code sẽ để trống."
            )
            df["Bank_Code"] = None
            return df

        # ── FIX: PostgreSQL trả về tên cột lowercase ─────────────────────
        df_bank = df_bank.copy()
        df_bank.columns = [c.lower() for c in df_bank.columns]
        # ──────────────────────────────────────────────────────────────────

        if _DB_CODE_COL not in df_bank.columns or _DB_NAME_COL not in df_bank.columns:
            logger.warning(
                "[Dim_AccountNumber] Dim_Bank thiếu cột '%s' hoặc '%s'. "
                "Các cột hiện có: %s. Bank_Code sẽ để trống.",
                _DB_CODE_COL, _DB_NAME_COL, df_bank.columns.tolist(),
            )
            df["Bank_Code"] = None
            return df

        # Build mapping: normalized_name → bank_code
        df_bank = df_bank.dropna(subset=[_DB_CODE_COL]).copy()
        df_bank["_norm"] = df_bank[_DB_NAME_COL].apply(_normalize_bank_name)
        bank_mapping_dict = dict(zip(df_bank["_norm"], df_bank[_DB_CODE_COL]))

        # Map vào df
        df["_norm"] = df["Bank_Name"].apply(_normalize_bank_name)
        df["Bank_Code"] = df["_norm"].map(bank_mapping_dict)
        df = df.drop(columns=["_norm"])

        # Log các NH không map được để dễ bổ sung vào Danh_sach_ngan_hang.xlsx
        null_mask = df["Bank_Code"].isna()
        if null_mask.any():
            unmatched = df.loc[null_mask, "Bank_Name"].unique().tolist()
            logger.warning(
                "[Dim_AccountNumber] %d/%d dòng không tìm được Bank_Code. "
                "Tên NH chưa có trong Dim_Bank: %s",
                null_mask.sum(), len(df), unmatched,
            )

        return df