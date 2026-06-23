"""Fact_BusinessPlan: Kế hoạch kinh doanh — unpivot (melt) các cột T1..T12 thành dòng theo tháng."""
import logging
import re

import pandas as pd

from .base import BaseTransformer, TransformContext

logger = logging.getLogger(__name__)


class FactBusinessPlanTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        df_raw = ctx.df_raw

        # 1. Định vị cột Khoản mục và các cột Tháng (T1 -> T12)
        item_col = None
        month_cols = []

        for col in df_raw.columns:
            col_str = str(col).strip()
            if col_str.lower() == "khoản mục":
                item_col = col
            # Dùng Regex để tìm đúng các cột T1, T2, ... T12
            elif re.match(r'^T\d{1,2}$', col_str, re.IGNORECASE):
                month_cols.append(col)

        if not item_col or not month_cols:
            logger.error("Không tìm thấy cột 'Khoản mục' hoặc các cột 'T1..T12' trong file kế hoạch!")

        # 2. UNPIVOT (Melt): Bẻ gập 12 cột tháng thành 2 cột (Tháng và Target_Amount)
        df_melted = pd.melt(
            df_raw,
            id_vars=[item_col],
            value_vars=month_cols,
            var_name='Month_Str',
            value_name='Target_Amount'
        )

        # Xóa các dòng rác không có tên khoản mục
        df_melted = df_melted.dropna(subset=[item_col])
        df_melted = df_melted[df_melted[item_col].astype(str).str.strip() != '']

        # 3. Chuyển đổi 'T1', 'T2'... thành định dạng Ngày (VD: '2026-01-01')
        # Ở đây ta lấy năm 2026 làm gốc (bạn có thể bắt động từ tên file nếu muốn)
        def convert_to_date(t_str):
            month_num = re.sub(r'[^\d]', '', t_str)
            return f"2026-{int(month_num):02d}-01"

        df_melted['Month'] = pd.to_datetime(df_melted['Month_Str'].apply(convert_to_date)).dt.date

        # 4. TỪ ĐIỂN MAPPING (Tự động gán mã Chỉ tiêu B02-DN dựa trên tên Khoản mục)
        def map_indicator_code(name):
            name_lower = str(name).lower().strip()
            if "doanh thu bán thành phẩm" in name_lower or "doanh thu" in name_lower and "thuần" not in name_lower:
                return "B02-DN_01"
            elif "giảm trừ" in name_lower:
                return "B02-DN_02"
            elif "doanh thu thuần" in name_lower:
                return "B02-DN_10"
            elif "giá vốn" in name_lower:
                return "B02-DN_11"
            elif "lợi nhuận gộp" in name_lower:
                return "B02-DN_20"
            elif "doanh thu hoạt động tài chính" in name_lower:
                return "B02-DN_22"
            elif "chi phí tài chính" in name_lower:
                return "B02-DN_23"
            elif "chi phí lãi vay" in name_lower:
                return "B02-DN_24"
            elif "chi phí bán hàng" in name_lower:
                return "B02-DN_25"
            elif "chi phí quản lý" in name_lower:
                return "B02-DN_26"
            elif "lợi nhuận thuần" in name_lower or "lntt từ hđkd" in name_lower:
                return "B02-DN_30"
            elif "thu nhập khác" in name_lower:
                return "B02-DN_31"
            elif "chi phí khác" in name_lower:
                return "B02-DN_32"
            elif "lợi nhuận khác" in name_lower:
                return "B02-DN_40"
            elif "tổng lợi nhuận" in name_lower or "lntt" in name_lower:
                return "B02-DN_50"
            elif "thuế tndn" in name_lower:
                return "B02-DN_51"
            elif "lợi nhuận sau thuế" in name_lower or "lnst" in name_lower:
                return "B02-DN_60"
            else:
                return None

        df_melted['Indicator_Code'] = df_melted[item_col].apply(map_indicator_code)

        # Giữ lại các dòng map được mã và có số liệu KPI
        df_melted = df_melted.dropna(subset=['Indicator_Code'])
        df_melted['Target_Amount'] = pd.to_numeric(df_melted['Target_Amount'], errors='coerce').fillna(0)

        # LỌC RÁC: Chỉ giữ lại các cột chuẩn của bảng Fact trước khi ném xuống Database
        return df_melted[['Indicator_Code', 'Month', 'Target_Amount']]
