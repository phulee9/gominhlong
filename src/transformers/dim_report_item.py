"""
Dim_ReportItem: dựng cây chỉ tiêu báo cáo tài chính (B01-DN / B02-DN) từ file gốc.

Thuật toán giữ nguyên 100% so với bản gốc (đã chạy ổn với dữ liệu thật):
  - Quét động cột "Mã số" và cột "Tên chỉ tiêu" để chống lỗi merged cells
  - Lọc rác (chữ ký, tiêu đề, dòng phân trang) theo từ khóa
  - Chặn nhiễm chéo B01/B02 nếu 2 báo cáo dính chung 1 sheet
  - Tự sinh mã NODE_x cho các dòng nhóm lớn không có mã số
  - Dựng quan hệ Parent_ID bằng stack theo cấp độ (level)
"""
import logging

import pandas as pd

from .base import BaseTransformer, TransformContext

logger = logging.getLogger(__name__)


class DimReportItemTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        df_raw = ctx.df_raw
        report_type = "B01-DN" if "b01" in ctx.file_id else "B02-DN"

        # Quét tìm động cột Mã số và cột Tên chỉ tiêu để chống lỗi ô gộp (Merged cells)
        col_code_name = None
        col_text_name = None

        for col in df_raw.columns:
            col_str = str(col).lower()
            if "mã số" in col_str or "mã" in col_str:
                col_code_name = col
            elif "chỉ tiêu" in col_str or "tài sản" in col_str or "nguồn vốn" in col_str:
                if col_text_name is None:
                    col_text_name = col

        if not col_code_name:
            col_code_name = df_raw.columns[1]
        if not col_text_name:
            col_text_name = df_raw.columns[0]

        processed_rows = []
        sort_idx = 1
        parent_stack = []  # Lưu tuple: (level, item_id)

        # BỘ LỌC ĐỘC QUYỀN: Loại bỏ hoàn toàn các dòng rác phân trang, chữ ký kế toán
        noise_keywords = [
            "tập đoàn gỗ", "nghĩa trụ", "asp kế toán", "người lập", "ký, họ tên", "kế toán trưởng",
            "người đại diện", "nguyễn minh hải", "phê duyệt", "đơn vị tính", "giả định hoạt động",
            "tại ngày", "kèm theo thông tư", "mẫu số:", "ghi chú:"
        ]

        for idx, row in df_raw.iterrows():
            raw_code = str(row.get(col_code_name, "")).strip()
            raw_name = str(row.get(col_text_name, "")).strip()

            # 1. Bỏ qua dòng trống hoặc dòng đánh số thứ tự cột kế toán (1, 2, 3, 4, 5)
            if pd.isna(row.get(col_text_name)) or raw_name == "" or raw_name.lower() in ["chỉ tiêu", "tài sản", "nguồn vốn", "1", "2", "3", "4", "5"]:
                continue

            name_lower = raw_name.lower()

            # 2. KIỂM TRA CHỐNG NHIỄM ĐỘC CHÉO: Nếu file B01 dính đoạn B02 ở dưới -> Chặt đuôi dừng luôn!
            if report_type == "B01-DN" and "kết quả hoạt động kinh doanh" in name_lower:
                logger.info("-> Phát hiện đoạn B02 lồng trong file B01. Tiến hành ngắt luồng đọc để tránh bẩn data!")
                break
            if report_type == "B02-DN" and "tình hình tài chính" in name_lower:
                continue

            # 3. Loại bỏ dòng rác tiêu đề/chữ ký nếu dính từ khóa rác
            if any(kw in name_lower for kw in noise_keywords):
                continue

            # Làm sạch mã số
            item_code = raw_code if (raw_code and raw_code != "nan" and raw_code != "") else ""

            # 4. Tiêu chuẩn của một dòng chỉ tiêu hợp lệ: có mã HOẶC bắt đầu bằng mã phân cấp (A., B., I., 1., -)
            is_valid_indicator = (
                item_code != "" or
                raw_name.isupper() or
                any(raw_name.startswith(p) for p in ["A.", "B.", "C.", "D.", "I.", "II.", "III.", "IV.", "V.", "VI.", "VII."]) or
                any(raw_name.strip().startswith(p) for p in ["1.", "2.", "3.", "4.", "5.", "6.", "7.", "8.", "9.", "- "])
            )

            if not is_valid_indicator:
                continue

            # Nếu mã trống nhưng là nhóm lớn (A., B...) thì đặt mã thông minh theo chữ cái đầu thay vì GRP ngẫu nhiên
            if item_code == "":
                first_word = raw_name.split()[0].replace(".", "").replace("-", "")
                item_code = f"NODE_{first_word}_{sort_idx}"

            item_id = f"{report_type}_{item_code}"

            # Phân cấp bậc cây chỉ tiêu
            current_level = 1
            if raw_name.isupper() or any(raw_name.startswith(p) for p in ["A.", "B.", "C -", "D -", "C.", "D."]):
                current_level = 0
            elif any(raw_name.startswith(roman) for roman in ["I.", "II.", "III.", "IV.", "V.", "VI.", "VII."]):
                current_level = 1
            elif len(item_code) > 2 and not item_code.startswith("NODE"):
                current_level = 2

            # Tìm nút cha từ Stack
            while parent_stack and parent_stack[-1][0] >= current_level:
                parent_stack.pop()
            parent_id = parent_stack[-1][1] if parent_stack else None

            # Đẩy nút hiện tại vào Stack
            parent_stack.append((current_level, item_id))

            # Ghi nhận dòng sạch
            processed_rows.append({
                "Item_ID": item_id,
                "Report_Type": report_type,
                "Item_Code": item_code if not item_code.startswith("NODE") else None,
                "Item_Name": raw_name,
                "Parent_ID": parent_id,
                "Sort_Index": sort_idx
            })
            sort_idx += 1

        return pd.DataFrame(processed_rows)
