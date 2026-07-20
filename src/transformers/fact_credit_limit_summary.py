"""
Fact_CreditLimitSummary: Hạn mức tín dụng theo ngân hàng.

Sheet "Theo dõi vay NH" có 2 bảng chồng nhau (đã xác nhận bằng cách đọc trực
tiếp file Excel thật):
  - Bảng 1 (dòng 3-9 Excel, index 0-6 sau header): Tổng hợp theo NH -> CẦN LẤY
  - Dòng 10 Excel (index 7): dòng "Tổng" -> cột Bank_Code (NH) ở dòng này là
    NaN, chữ "Tổng" thực ra nằm ở cột STT, KHÔNG nằm ở cột Bank_Code.
  - Dòng 11 Excel (index 8): trống
  - Dòng 12 Excel (index 9): header phụ của Bảng 2 ("STT, NH, Nhập Số khế
    ước...") bị đọc lẫn vào như 1 dòng dữ liệu vì pandas chỉ dùng 1 header
    duy nhất (dòng 1) cho toàn sheet.
  - Dòng 13+ : "Cộng vay NH", "Cộng vay MB", rồi chi tiết từng khế ước vay
    (Bank_Code lặp lại MB/VP/TP...) -> Bảng 2, KHÔNG liên quan, phải loại bỏ.

=> Phải dùng cột STT (không phải Bank_Code) để xác định dòng "Tổng" và cắt
   tại đó. Cột STT cần được khai báo thêm trong field_mapping của
   pipeline_config.yaml (source: "STT" -> target: "STT") chỉ để dùng nội bộ
   ở đây; bị xoá khỏi df trước khi trả về nên không ảnh hưởng DB.

LOOKUP Credit_Limit/Limit_Type/Interest_Rate từ sheet "Tổng hợp" (cùng file):
Sheet "Tổng hợp" có 2 NHÓM riêng biệt, phân biệt bởi dòng tiêu đề nhóm:
  - "A. HMTD NGẮN HẠN" (dòng 6) -> các dòng NH bên dưới (dòng 7-14) là vay
    Ngắn hạn: MB, VP, TP, IVB, Worri, HD, SCB (7 NH).
  - "A. HMTD DÀI HẠN" (dòng 15) -> các dòng NH bên dưới (dòng 16-17) là vay
    Dài hạn: chỉ có MB (55,457,457,500).
MB xuất hiện ở CẢ 2 nhóm với Credit_Limit/Interest_Rate khác nhau. Một dict
khoá theo Bank_Code đơn thuần sẽ bị ghi đè (dòng đọc sau cùng thắng) ->
PHẢI khoá theo (Bank_Code, Limit_Type) để giữ được cả 2 bản ghi của MB.

Bảng "Theo dõi vay NH" (Bảng 1, nguồn chính của Fact) chỉ có 1 dòng MB với
Granted_Limit = 90 tỷ -> đây là phần NGẮN HẠN của MB (khớp đúng theo
Credit_Limit Ngắn hạn = 90 tỷ ở sheet Tổng hợp). Phần DÀI HẠN của MB
(55.4 tỷ) KHÔNG có dòng tương ứng trong "Theo dõi vay NH" -> phải tự thêm
1 dòng mới cho MB-Dài hạn để không bị bỏ sót 1 khoản hạn mức tín dụng thật.
Dữ liệu cho dòng thêm này lấy trực tiếp từ sheet "Tổng hợp" dòng MB-Dài hạn:
  Granted_Limit = Hạn mức vay (cột D) = 55,457,457,500
  Principal_Balance = Dư nợ gốc vay đến hiện tại (cột J) = 34,210,739,042
  Remaining_Disbursement = Granted_Limit - Principal_Balance (tính toán,
    vì sheet "Tổng hợp" không có cột "hạn mức còn được giải ngân" riêng
    cho dòng MB-Dài hạn, chỉ có ở dòng tổng "I. Ngân hàng").

Lưu ý: đọc sheet "Tổng hợp" TRONG CÙNG FILE (ctx.file_path) — không phải
file sidecar khác, nên không có vấn đề gãy khi chạy qua MinIO/Airflow (file
đã được download nguyên vẹn về local trước khi đọc, sheet "Tổng hợp" vẫn
nằm trong cùng workbook đó).
"""
import logging

import pandas as pd

from .base import BaseTransformer, TransformContext

logger = logging.getLogger(__name__)


class FactCreditLimitSummaryTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        df = df.reset_index(drop=True)

        # 0. Tìm dòng "Tổng" dựa vào cột STT (KHÔNG dùng Bank_Code — ở dòng
        #    "Tổng" cột Bank_Code/NH luôn là NaN, chữ "Tổng" nằm ở cột STT).
        #    Mọi dòng TỪ dòng này trở đi (kể cả Bảng 2 - chi tiết khế ước)
        #    đều bị loại bỏ. Không hardcode số dòng/số ngân hàng -> vẫn đúng
        #    khi khách hàng thêm/bớt ngân hàng trong Bảng 1.
        if "STT" not in df.columns:
            raise ValueError(
                "fact_credit_limit_summary: thiếu cột 'STT' trong field_mapping "
                "của pipeline_config.yaml. Cột này BẮT BUỘC để transformer xác "
                "định đúng điểm cắt giữa Bảng 1 (tổng hợp theo NH) và Bảng 2 "
                "(chi tiết khế ước) nằm trong cùng sheet 'Theo dõi vay NH'."
            )

        stt_mask = df["STT"].astype(str).str.strip().str.lower().str.fullmatch(
            "tổng|cộng|total", na=False
        )

        if stt_mask.any():
            cutoff_idx = stt_mask.idxmax()
            df = df.iloc[:cutoff_idx].copy()
            logger.info(
                f"[fact_credit_limit_summary] Cắt tại dòng 'Tổng' (idx={cutoff_idx} "
                f"trong df), giữ {len(df)} dòng ngân hàng từ Bảng tổng hợp."
            )
        else:
            logger.warning(
                "[fact_credit_limit_summary] KHÔNG tìm thấy dòng 'Tổng' (cột STT) "
                "trong sheet 'Theo dõi vay NH' — cấu trúc file có thể đã thay đổi. "
                "Toàn bộ df sẽ được giữ lại NGUYÊN, có nguy cơ lẫn dữ liệu Bảng 2 "
                "(chi tiết khế ước) -> CẦN kiểm tra lại file/cấu hình thủ công."
            )

        # Xoá cột STT sau khi dùng xong — không phải cột nghiệp vụ, không insert DB
        df = df.drop(columns=["STT"], errors="ignore")

        # 1. Lọc tiếp các dòng rỗng/None như cũ (an toàn thêm, dù bước 0 đã
        #    cắt đúng vị trí — phòng trường hợp có dòng trống lẫn trong Bảng 1)
        df = df[df["Bank_Code"].notna()]
        df["Bank_Code"] = df["Bank_Code"].astype(str).str.strip()
        df = df[df["Bank_Code"] != ""]
        df = df[df["Bank_Code"].str.lower() != "nan"]
        df = df[
            ~df["Bank_Code"].str.lower().str.contains("tổng|cộng|total", na=False)
        ]

        # 2. Lookup Credit_Limit, Limit_Type, Interest_Rate từ sheet "Tổng hợp"
        #    Sheet này có NHIỀU bảng khác nhau, chỉ bảng ĐẦU TIÊN (bắt đầu
        #    từ dòng "A. HMTD NGẮN HẠN") là phần phân nhóm Ngắn hạn/Dài hạn
        #    cấp ngân hàng mà ta cần. Các bảng phía dưới (vd "NGÂN HÀNG |
        #    Tổng HMTD | ...", "HMTD theo tài sản", "HMTD theo hợp đồng")
        #    là bảng tổng hợp KHÁC, không liên quan -> PHẢI dừng đọc trước
        #    khi lẫn vào các bảng này, nếu không current_limit_type cũ vẫn
        #    còn hiệu lực và gán nhầm "Dài hạn" cho toàn bộ NH ở các bảng đó.
        #    Mốc dừng: gặp lại dòng có "NGÂN HÀNG" ở cột tên NH (header của
        #    bảng kế tiếp) HOẶC 2 dòng trống liên tiếp sau khi đã có dữ liệu.
        bank_info = {}  # {(bank_code, limit_type): {credit_limit, interest_rate, principal, ...}}
        try:
            df_tong_hop = pd.read_excel(
                ctx.file_path, sheet_name="Tổng hợp", header=None, engine="openpyxl"
            )

            current_limit_type = None
            started = False  # đã gặp "A. HMTD NGẮN HẠN" hay chưa
            for _, row in df_tong_hop.iterrows():
                row_vals = [str(v).strip() if v is not None else "" for v in row.values]
                row_text = " ".join(row_vals).upper()

                # Mốc bắt đầu
                if "HMTD" in row_text and ("NGẮN HẠN" in row_text or "NGAN HAN" in row_text):
                    current_limit_type = "Ngắn hạn"
                    started = True
                    continue
                if "HMTD" in row_text and ("DÀI HẠN" in row_text or "DAI HAN" in row_text):
                    current_limit_type = "Dài hạn"
                    continue

                if not started:
                    continue  # chưa tới bảng cần đọc, bỏ qua mọi dòng phía trên

                # Mốc DỪNG: gặp lại header "NGÂN HÀNG" của 1 bảng khác phía
                # dưới (vd "NGÂN HÀNG | Tổng HMTD | Số dư HMTD sử dụng...")
                bank_col_val = row_vals[2] if len(row_vals) > 2 else ""
                if bank_col_val.upper() == "NGÂN HÀNG":
                    break

                # Dòng dữ liệu: cột B (index 1) là số thứ tự (float),
                # cột C (index 2) là tên NH
                stt_val = row.iloc[1]
                bank_val = str(row.iloc[2]).strip() if row.iloc[2] else ""
                credit_limit = row.iloc[3]        # Cột D: Hạn mức vay
                interest_rate = row.iloc[6]       # Cột G: Lãi suất
                # Cột J (index 9): Dư nợ gốc vay đến hiện tại — chỉ có ý
                # nghĩa cho nhóm Dài hạn (dùng để tạo dòng Fact mới, vì
                # "Theo dõi vay NH" không có dòng MB-Dài hạn tương ứng)
                principal_balance = row.iloc[9] if len(row) > 9 else None

                try:
                    float(stt_val)  # Chỉ xử lý dòng có STT là số (loại dòng tổng/nhóm/"I. Vay ngắn hạn"...)
                    if bank_val and bank_val.lower() != "nan" and current_limit_type:
                        bank_info[(bank_val, current_limit_type)] = {
                            "Credit_Limit": credit_limit,
                            "Limit_Type": current_limit_type,
                            "Interest_Rate": interest_rate,
                            "Principal_Balance": principal_balance,
                        }
                except (ValueError, TypeError):
                    continue

        except Exception as e:
            logger.warning(f"Lỗi khi đọc sheet Tổng hợp để lookup: {e}")

        # 2a. Join Credit_Limit/Limit_Type/Interest_Rate vào các dòng đã có
        #     (7 dòng từ "Theo dõi vay NH") — match theo Bank_Code, ưu tiên
        #     nhóm "Ngắn hạn" nếu Bank_Code có ở cả 2 nhóm (vì Bảng 1 chỉ
        #     phản ánh phần Ngắn hạn — đã xác nhận qua đối chiếu Credit_Limit
        #     90 tỷ của MB khớp đúng nhóm Ngắn hạn, không khớp nhóm Dài hạn).
        def _pick_info(bank_code: str) -> dict:
            if (bank_code, "Ngắn hạn") in bank_info:
                return bank_info[(bank_code, "Ngắn hạn")]
            # fallback: NH không có nhóm Ngắn hạn (không xảy ra trong file
            # mẫu hiện tại, nhưng giữ để an toàn cho file khách hàng khác)
            for (bc, lt), info in bank_info.items():
                if bc == bank_code:
                    return info
            return {}

        df["Credit_Limit"] = df["Bank_Code"].map(lambda b: _pick_info(b).get("Credit_Limit"))
        df["Limit_Type"] = df["Bank_Code"].map(lambda b: _pick_info(b).get("Limit_Type"))
        df["Interest_Rate"] = df["Bank_Code"].map(lambda b: _pick_info(b).get("Interest_Rate"))

        # 2b. Thêm dòng riêng cho các (Bank_Code, "Dài hạn") CHƯA có trong df
        #     (vd: MB-Dài hạn 55.4 tỷ không có dòng tương ứng trong "Theo
        #     dõi vay NH") — để không bỏ sót khoản hạn mức tín dụng thật.
        existing_banks = set(df["Bank_Code"])
        extra_rows = []
        for (bank_code, limit_type), info in bank_info.items():
            if limit_type != "Dài hạn":
                continue

            granted_limit = info.get("Credit_Limit")
            # Bỏ qua các dòng không có Credit_Limit thật (vd "Chailease",
            # "Viettinbank leasing" trong mục "II. Leasing" — không có số
            # liệu cụ thể, không phải khoản hạn mức tín dụng cần phản ánh)
            try:
                if granted_limit is None or pd.isna(granted_limit) or float(granted_limit) <= 0:
                    continue
            except (TypeError, ValueError):
                continue

            # NH này đã có dòng Dài hạn join đúng ở bước 2a? -> kiểm tra
            # xem dòng hiện có của bank_code đang mang Limit_Type gì.
            current_type_of_bank = df.loc[df["Bank_Code"] == bank_code, "Limit_Type"]
            if bank_code in existing_banks and (current_type_of_bank == "Dài hạn").any():
                continue  # đã có dòng Dài hạn riêng cho NH này rồi, không thêm trùng

            principal_balance = info.get("Principal_Balance")
            remaining = None
            try:
                if granted_limit is not None and principal_balance is not None:
                    remaining = float(granted_limit) - float(principal_balance)
            except (TypeError, ValueError):
                remaining = None

            extra_rows.append({
                "Bank_Code": bank_code,
                "Granted_Limit": granted_limit,
                "Principal_Balance": principal_balance,
                "Remaining_Disbursement": remaining,
                "Credit_Limit": granted_limit,
                "Limit_Type": limit_type,
                "Interest_Rate": info.get("Interest_Rate"),
            })
            logger.info(
                f"[fact_credit_limit_summary] Thêm dòng '{bank_code}' nhóm "
                f"'{limit_type}' (không có trong 'Theo dõi vay NH', lấy từ "
                f"sheet 'Tổng hợp')."
            )

        if extra_rows:
            df = pd.concat([df, pd.DataFrame(extra_rows)], ignore_index=True)

        # 4. ID tự tăng
        df = df.reset_index(drop=True)
        df["ID"] = df.index + 1
        df["Bank_Code"] = df["Bank_Code"].str.upper()

        return df