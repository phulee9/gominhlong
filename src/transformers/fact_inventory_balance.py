"""Fact_Inventory_Balance: Tổng hợp tồn kho — quét Snapshot_Date từ dòng tiêu đề, lọc rác, đánh ID."""
import logging
import re

import pandas as pd

from .base import BaseTransformer, TransformContext

logger = logging.getLogger(__name__)


class FactInventoryBalanceTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        # 1. Quét tìm Snapshot_Date từ các dòng tiêu đề gốc (trước khi bị pandas cắt đi)
        snapshot_date = None
        try:
            # Đọc tạm 10 dòng đầu tiên để phân tích text
            df_head_temp = pd.read_excel(
                ctx.file_path, sheet_name=ctx.sheet, nrows=10, header=None, engine="openpyxl"
            )
            for _, r in df_head_temp.iterrows():
                for cell in r.values:
                    cell_str = str(cell).lower()
                    # Tìm chuỗi có chứa cụm 'đến ngày dd/mm/yyyy'
                    if "đến ngày" in cell_str:
                        match = re.search(r'đến ngày\s+(\d{1,2}/\d{1,2}/\d{4})', cell_str)
                        if match:
                            snapshot_date = match.group(1)
                            break
                if snapshot_date:
                    break
        except Exception as e:
            logger.warning(f"Lỗi khi quét tìm Snapshot_Date: {e}")

        # Gán ngày tháng vào DataFrame
        if snapshot_date:
            df['Snapshot_Date'] = pd.to_datetime(snapshot_date, format="%d/%m/%Y").date()
        else:
            logger.warning("Không tìm thấy chuỗi 'đến ngày dd/mm/yyyy' trong file!")
            df['Snapshot_Date'] = None

        # 2. Làm sạch dòng rác và đánh ID
        df = df.dropna(subset=['Warehouse_Code', 'Product_Code'])
        df = df.reset_index(drop=True)
        df['ID'] = df.index + 1
        return df
