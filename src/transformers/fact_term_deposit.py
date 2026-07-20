"""Fact_TermDeposit: Hợp đồng tiền gửi — dọn dòng tổng nhóm, chuẩn hoá lãi suất, đánh ID."""
import pandas as pd
from .base import BaseTransformer, TransformContext


class FactTermDepositTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        # --- 1. Loại bỏ "dòng tổng nhóm" (subtotal rows) ---
        # File gốc có các dòng tổng theo công ty/ngân hàng (vd: SỐ SỔ = 'MINH LONG',
        # 'MB', 'HD', 'LP', 'AGR', 'IVB', 'WOORI', 'VP'...). Các dòng này KHÔNG
        # rỗng ở cột Passbook_No nên dropna(subset=['Passbook_No']) cũ không lọc
        # được. Tín hiệu đáng tin cậy để nhận diện dòng tổng là cột STT (cột A,
        # đã được map vào df_raw) luôn trống ở dòng tổng, và luôn có số ở dòng
        # hợp đồng thật. Ta lấy cột STT từ df_raw (chưa bị field_mapping lọc bỏ)
        # qua ctx.df_raw để filter, tránh phụ thuộc vào field_mapping có khai
        # báo STT hay không.
        stt_col_candidates = [c for c in ctx.df_raw.columns if str(c).strip().upper() == "STT"]
        if stt_col_candidates:
            stt_col = stt_col_candidates[0]
            stt_series = ctx.df_raw[stt_col]
            # Dòng hợp đồng hợp lệ: STT parse được thành số (vd 1, 2, 3...)
            valid_mask = pd.to_numeric(stt_series, errors="coerce").notna()
            df = df.loc[valid_mask.reindex(df.index, fill_value=False)]
        else:
            # Fallback (không tìm thấy cột STT): giữ logic cũ nhưng siết thêm
            # điều kiện Interest_Rate/Term phải có giá trị, vì dòng tổng luôn
            # rỗng 2 cột này còn dòng hợp đồng thật luôn có.
            df = df.dropna(subset=["Passbook_No"])
            if "Interest_Rate" in df.columns and "Term" in df.columns:
                df = df.dropna(subset=["Interest_Rate", "Term"], how="all")

        # Vẫn dọn các dòng hoàn toàn trống ở Passbook_No (đề phòng)
        df = df.dropna(subset=["Passbook_No"])

        # --- 2. Xử lý lãi suất (bỏ dấu % nếu có) ---
        if "Interest_Rate" in df.columns:
            df["Interest_Rate"] = (
                df["Interest_Rate"]
                .astype(str)
                .str.replace("%", "", regex=False)
                .str.replace(",", ".", regex=False)
                .str.strip()
            )
            df["Interest_Rate"] = pd.to_numeric(df["Interest_Rate"], errors="coerce")

        # --- 3. Chuẩn hoá các cột ngày dạng string dd/mm/yyyy ---
        # File gốc lẫn cả 2 kiểu: ô Excel kiểu Date thật (openpyxl/pandas đã
        # parse sẵn thành Timestamp/ "yyyy-mm-dd HH:MM:SS") và ô kiểu Text
        # "dd/mm/yyyy" (vd '01/11/2024'). Nếu ép dayfirst=True cho TOÀN BỘ cột,
        # các giá trị đã là Timestamp đúng (vd 2025-04-11 = 11/4) sẽ bị
        # to_datetime hiểu lại theo dayfirst và đảo thành sai (2025-11-04).
        # Vì vậy chỉ áp dayfirst=True cho các ô có dạng string "dd/mm/yyyy",
        # còn các ô đã ở dạng "yyyy-mm-dd..." (đến từ Timestamp gốc, vì cột đọc
        # dtype=str nên Timestamp bị str() thành "yyyy-mm-dd HH:MM:SS") thì parse
        # theo ISO, không dùng dayfirst.
        import re

        def _parse_date(value):
            if value is None or (isinstance(value, str) and value.strip() == ""):
                return pd.NaT
            text = str(value).strip()
            if re.match(r"^\d{4}-\d{2}-\d{2}", text):
                # Đến từ ô Excel kiểu Date thật, đã ở dạng ISO -> không dayfirst
                return pd.to_datetime(text, errors="coerce")
            # Đến từ ô Excel kiểu Text "dd/mm/yyyy" -> phải ép dayfirst
            return pd.to_datetime(text, errors="coerce", dayfirst=True)

        for date_col in ("Deposit_Date", "Maturity_Date", "Settlement_Date"):
            if date_col in df.columns:
                df[date_col] = df[date_col].apply(_parse_date)

        # --- 4. Bỏ cột phụ trợ STT (chỉ dùng để lọc dòng tổng ở bước 1,
        # không thuộc field nghiệp vụ của bảng Fact_TermDeposit, không load
        # vào DB) ---
        if "STT" in df.columns:
            df = df.drop(columns=["STT"])

        # --- 5. Tạo ID tự tăng ---
        df = df.reset_index(drop=True)
        df["ID"] = df.index + 1

        df["Bank_Code"] = df["Bank_Code"].str.upper()
        return df