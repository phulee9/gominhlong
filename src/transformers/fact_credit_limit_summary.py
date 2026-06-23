"""
Fact_CreditLimitSummary: Hạn mức tín dụng theo ngân hàng.

Lưu ý: lookup Credit_Limit/Limit_Type/Interest_Rate đọc từ sheet "Tổng hợp"
TRONG CÙNG FILE (ctx.file_path) — không phải file sidecar khác, nên không có
vấn đề gãy khi chạy qua MinIO/Airflow (file đã được download nguyên vẹn về
local trước khi đọc, sheet "Tổng hợp" vẫn nằm trong cùng workbook đó).
"""
import logging

import pandas as pd

from .base import BaseTransformer, TransformContext

logger = logging.getLogger(__name__)


class FactCreditLimitSummaryTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        # 1. Chỉ lấy dòng dữ liệu thật từ sheet "Theo dõi vay NH" (có Bank_Code là tên NH)
        # Loại bỏ dòng "Tổng", dòng trống
        df = df[df['Bank_Code'].notna()]
        df = df[~df['Bank_Code'].astype(str).str.lower().str.contains('tổng|cộng|total', na=False)]
        df['Bank_Code'] = df['Bank_Code'].astype(str).str.strip()
        df = df[df['Bank_Code'] != '']

        # 2. Lookup Credit_Limit, Limit_Type, Interest_Rate từ sheet "Tổng hợp"
        try:
            df_tong_hop = pd.read_excel(
                ctx.file_path, sheet_name='Tổng hợp',
                header=None, engine='openpyxl'
            )

            # Xác định Limit_Type: dòng có "NGẮN HẠN" → các dòng sau là Ngắn hạn,
            # dòng có "DÀI HẠN" → các dòng sau là Dài hạn
            current_limit_type = None
            bank_info = {}  # {bank_code: {credit_limit, limit_type, interest_rate}}

            for _, row in df_tong_hop.iterrows():
                row_vals = [str(v).strip() if v is not None else '' for v in row.values]
                row_text = ' '.join(row_vals).upper()

                # Detect nhóm hạn mức
                if 'NGẮN HẠN' in row_text or 'NGAN HAN' in row_text:
                    current_limit_type = 'Ngắn hạn'
                    continue
                if 'DÀI HẠN' in row_text or 'DAI HAN' in row_text:
                    current_limit_type = 'Dài hạn'
                    continue

                # Dòng dữ liệu: cột B là số thứ tự (float), cột C là tên NH
                stt_val = row.iloc[1]
                bank_val = str(row.iloc[2]).strip() if row.iloc[2] else ''
                credit_limit = row.iloc[3]   # Cột D: Hạn mức vay
                interest_rate = row.iloc[6]  # Cột G: Lãi suất

                try:
                    float(stt_val)  # Chỉ xử lý dòng có STT là số
                    if bank_val and bank_val != 'nan':
                        bank_info[bank_val.strip()] = {
                            'Credit_Limit': credit_limit,
                            'Limit_Type': current_limit_type,
                            'Interest_Rate': interest_rate
                        }
                except (ValueError, TypeError):
                    continue

            # 3. Join thông tin vào df chính
            df['Credit_Limit'] = df['Bank_Code'].map(
                lambda b: bank_info.get(b, {}).get('Credit_Limit')
            )
            df['Limit_Type'] = df['Bank_Code'].map(
                lambda b: bank_info.get(b, {}).get('Limit_Type')
            )
            df['Interest_Rate'] = df['Bank_Code'].map(
                lambda b: bank_info.get(b, {}).get('Interest_Rate')
            )

        except Exception as e:
            logger.warning(f"Lỗi khi đọc sheet Tổng hợp để lookup: {e}")
            df['Credit_Limit'] = None
            df['Limit_Type'] = None
            df['Interest_Rate'] = None

        # 4. ID tự tăng
        df = df.reset_index(drop=True)
        df['ID'] = df.index + 1
        return df
