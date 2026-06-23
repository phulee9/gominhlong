"""
base.py - Interface chung cho mọi transformer xử lý business logic riêng của 1 target_table.

Trước đây toàn bộ logic này (17+ nhánh) nằm trong 1 hàm if/elif khổng lồ bên
trong ExcelReader._read_one_sheet(). Vấn đề:
  - Sửa logic 1 bảng dễ ảnh hưởng nhầm bảng khác (cùng 1 hàm, cùng scope biến)
  - Không test riêng được từng bảng
  - Không biết bảng nào có logic đặc biệt nếu không đọc hết 800 dòng

Giờ mỗi bảng có 1 transformer riêng trong package này, khai báo trong
pipeline_config.yaml qua field `transformer_class` (đã có sẵn trong
SheetConfig, trước đây không dùng tới). ExcelReader chỉ còn việc:
  1. Đọc Excel + áp field_mapping (chọn cột, rename) — KHÔNG đổi
  2. Nếu sheet_cfg.transformer_class có giá trị -> gọi transformer tương ứng
  3. Cast dtype + dọn NaN — KHÔNG đổi

Quy ước: transform() nhận df ĐÃ qua field_mapping (cột đã rename sang tên
target), và trả về df đã áp dụng xong toàn bộ logic riêng (lọc dòng rác,
tách cột, unpivot, build ID, lookup...). KHÔNG cast dtype, KHÔNG dropna(how="all")
trong transformer — 2 việc đó ExcelReader vẫn làm sau, áp dụng chung cho mọi bảng.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

import pandas as pd


@dataclass
class TransformContext:
    file_path: str            # đường dẫn local của file Excel đang xử lý (đã download từ MinIO nếu có)
    file_id: str               # file_id trong config (vd: "dim_partner_khach_hang")
    sheet: object               # sheet_name hoặc sheet_index đã dùng để đọc sheet này
    sheet_cfg: object           # SheetConfig gốc
    file_config: object         # ExcelFileConfig gốc
    df_raw: pd.DataFrame        # dataframe TRƯỚC field_mapping (giữ tên cột gốc trong Excel)
    lookup: Optional[Callable[[str], pd.DataFrame]] = None
    # lookup(target_table) -> DataFrame chứa TOÀN BỘ bảng đã load trong DB (vd: lookup("Dim_Bank")).
    # Dùng khi 1 bảng cần tra cứu dữ liệu của bảng khác ĐÃ LOAD (thay cho đọc file Excel sidecar).
    # Trả về None nếu không có kết nối DB (vd: dry-run); transformer cần tự xử lý trường hợp này.


class BaseTransformer:
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        raise NotImplementedError
