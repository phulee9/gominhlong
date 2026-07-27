"""
Fact_CreditLimitSummary: Hạn mức tín dụng theo ngân hàng.

LÀM ĐÚNG THEO MAPPING SPEC (Silver.Fact_CreditLimitSummary):

  Field                  | Nguồn                | Cột nguồn                    | Quy tắc
  -----------------------|----------------------|------------------------------|------------------
  ID                     | (sinh)               |                              | STT tăng dần
  Bank_Code              | Theo dõi vay NH      | NH (cột B)                   | dòng 1-9 (Bảng 1)
  Granted_Limit          | Theo dõi vay NH      | Tổng HMTD (cột C)            | dòng 1-9
  Principal_Balance      | Theo dõi vay NH      | Dư nợ gốc (cột D)            | dòng 1-9
  Remaining_Disbursement | Theo dõi vay NH      | Số tiền còn được giải ngân   | dòng 1-9
                         |                      | (cột E)                      |
  Credit_Limit           | Tổng hợp             | Hạn mức vay (cột D)          | dòng 3-17
  Interest_Rate          | Tổng hợp             | Lãi suất(%) (cột G)          | dòng 3-17
  Limit_Type             | Tổng hợp             | dòng 6 ("A. HMTD NGẮN HẠN")  | gán theo nhóm
                         |                      | và dòng 15 ("A. HMTD DÀI HẠN")|

  => TOÀN BỘ dòng fact = các dòng NH của Bảng 1 sheet "Theo dõi vay NH".
     KHÔNG tự thêm dòng nào khác (bản cũ tự chế dòng MB-Dài hạn với
     Granted_Limit/Principal_Balance lấy từ "Tổng hợp" và
     Remaining_Disbursement tự tính — SAI nguồn theo mapping, đã bỏ).
     Sheet "Tổng hợp" CHỈ dùng để lookup 3 trường:
     Credit_Limit, Limit_Type, Interest_Rate.

CẤU TRÚC FILE NGUỒN (đã xác nhận trên file thật):
  Sheet "Theo dõi vay NH" có 2 bảng chồng nhau:
    - Bảng 1 (dòng 3-9 Excel): tổng hợp theo NH -> nguồn chính của fact.
    - Dòng "Tổng": chữ "Tổng" nằm ở cột STT (cột NH là NaN) -> cắt tại đây.
    - Bảng 2 (phía dưới): chi tiết từng khế ước -> loại bỏ toàn bộ.
    Cột STT phải khai báo trong field_mapping (source "STT" -> target "STT")
    chỉ dùng nội bộ để tìm điểm cắt; xoá trước khi trả về.
  Sheet "Tổng hợp":
    - Dòng "A. HMTD NGẮN HẠN" mở nhóm Ngắn hạn (các NH: MB, VP, TP, ...).
    - Dòng "A. HMTD DÀI HẠN"  mở nhóm Dài hạn (chỉ MB trong file mẫu).
    - MB xuất hiện ở CẢ 2 nhóm -> lookup phải khoá (Bank_Code, Limit_Type).
    - Khi 1 NH có ở cả 2 nhóm: chọn bản ghi có Credit_Limit khớp đúng
      Granted_Limit của dòng fact (MB: 90 tỷ -> nhóm Ngắn hạn); nếu không
      khớp được thì ưu tiên Ngắn hạn.
    - Dừng đọc khi gặp header "NGÂN HÀNG" của bảng kế tiếp phía dưới
      (tránh gán nhầm Limit_Type cho các bảng tổng hợp khác trong sheet).
"""
import logging

import pandas as pd

from .base import BaseTransformer, TransformContext

logger = logging.getLogger(__name__)


class FactCreditLimitSummaryTransformer(BaseTransformer):

    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        df = df.reset_index(drop=True)

        # ── 0. Cắt Bảng 1: dựa vào dòng "Tổng" ở cột STT ─────────────────
        #    (cột NH tại dòng "Tổng" là NaN nên KHÔNG dùng Bank_Code để tìm)
        if "STT" not in df.columns:
            raise ValueError(
                "fact_credit_limit_summary: thiếu cột 'STT' trong field_mapping "
                "của pipeline_config.yaml. Cột này BẮT BUỘC để xác định điểm cắt "
                "giữa Bảng 1 (tổng hợp theo NH) và Bảng 2 (chi tiết khế ước) "
                "trong cùng sheet 'Theo dõi vay NH'."
            )
        stt_mask = df["STT"].astype(str).str.strip().str.lower().str.fullmatch(
            "tổng|cộng|total", na=False
        )
        if stt_mask.any():
            cutoff_idx = stt_mask.idxmax()
            df = df.iloc[:cutoff_idx].copy()
            logger.info(
                f"[fact_credit_limit_summary] Cắt tại dòng 'Tổng' (idx={cutoff_idx}), "
                f"giữ {len(df)} dòng ngân hàng của Bảng 1."
            )
        else:
            logger.warning(
                "[fact_credit_limit_summary] KHÔNG tìm thấy dòng 'Tổng' (cột STT) — "
                "cấu trúc file có thể đã đổi, nguy cơ lẫn dữ liệu Bảng 2. "
                "CẦN kiểm tra file/cấu hình thủ công."
            )
        df = df.drop(columns=["STT"], errors="ignore")

        # ── 1. Làm sạch Bank_Code (nguồn: Theo dõi vay NH, cột NH) ───────
        df = df[df["Bank_Code"].notna()]
        df["Bank_Code"] = df["Bank_Code"].astype(str).str.strip()
        df = df[df["Bank_Code"] != ""]
        df = df[df["Bank_Code"].str.lower() != "nan"]
        df = df[
            ~df["Bank_Code"].str.lower().str.contains("tổng|cộng|total", na=False)
        ]

        # ── 2. Lookup Credit_Limit / Limit_Type / Interest_Rate ──────────
        #    Nguồn DUY NHẤT của 3 trường này: sheet "Tổng hợp" (dòng 3-17),
        #    khoá (Bank_Code, Limit_Type) vì MB có ở cả 2 nhóm.
        bank_info = {}  # {(bank_code, limit_type): {Credit_Limit, Interest_Rate}}
        try:
            df_tong_hop = pd.read_excel(
                ctx.file_path, sheet_name="Tổng hợp", header=None, engine="openpyxl"
            )
            current_limit_type = None
            started = False
            for _, row in df_tong_hop.iterrows():
                row_vals = [str(v).strip() if v is not None else "" for v in row.values]
                row_text = " ".join(row_vals).upper()

                # Mốc nhóm (mapping: dòng 6 = Ngắn hạn, dòng 15 = Dài hạn —
                # nhận diện theo chữ đánh dấu để không gãy khi thêm/bớt dòng)
                if "HMTD" in row_text and ("NGẮN HẠN" in row_text or "NGAN HAN" in row_text):
                    current_limit_type = "Ngắn hạn"
                    started = True
                    continue
                if "HMTD" in row_text and ("DÀI HẠN" in row_text or "DAI HAN" in row_text):
                    current_limit_type = "Dài hạn"
                    continue
                if not started:
                    continue

                # Mốc DỪNG: header "NGÂN HÀNG" của bảng khác phía dưới
                bank_col_val = row_vals[2] if len(row_vals) > 2 else ""
                if bank_col_val.upper() == "NGÂN HÀNG":
                    break

                # Dòng dữ liệu NH: cột B (idx 1) = STT số, cột C (idx 2) = tên NH
                stt_val = row.iloc[1]
                bank_val = str(row.iloc[2]).strip() if row.iloc[2] else ""
                credit_limit = row.iloc[3]    # Cột D: Hạn mức vay   (mapping)
                interest_rate = row.iloc[6]   # Cột G: Lãi suất(%)   (mapping)
                try:
                    float(stt_val)  # chỉ nhận dòng có STT là số (loại dòng nhóm/tổng)
                    if bank_val and bank_val.lower() != "nan" and current_limit_type:
                        bank_info[(bank_val, current_limit_type)] = {
                            "Credit_Limit": credit_limit,
                            "Interest_Rate": interest_rate,
                        }
                except (ValueError, TypeError):
                    continue
        except Exception as e:
            logger.warning(f"[fact_credit_limit_summary] Lỗi đọc sheet 'Tổng hợp': {e}")

        # ── 2a. Join vào từng dòng fact ──────────────────────────────────
        #    NH có ở cả 2 nhóm: chọn bản ghi có Credit_Limit == Granted_Limit
        #    của dòng fact; không khớp được -> ưu tiên Ngắn hạn -> bất kỳ.
        def _pick_info(bank_code: str, granted_limit) -> tuple:
            candidates = {
                lt: info for (bc, lt), info in bank_info.items() if bc == bank_code
            }
            if not candidates:
                return None, {}
            if len(candidates) > 1:
                try:
                    g = float(granted_limit)
                    for lt, info in candidates.items():
                        try:
                            if float(info["Credit_Limit"]) == g:
                                return lt, info
                        except (TypeError, ValueError):
                            continue
                except (TypeError, ValueError):
                    pass
            if "Ngắn hạn" in candidates:
                return "Ngắn hạn", candidates["Ngắn hạn"]
            lt = next(iter(candidates))
            return lt, candidates[lt]

        looked_up = df.apply(
            lambda r: _pick_info(r["Bank_Code"], r.get("Granted_Limit")), axis=1
        )
        df["Limit_Type"] = looked_up.map(lambda x: x[0])
        df["Credit_Limit"] = looked_up.map(lambda x: x[1].get("Credit_Limit"))
        df["Interest_Rate"] = looked_up.map(lambda x: x[1].get("Interest_Rate"))

        missing = df[df["Limit_Type"].isna()]["Bank_Code"].tolist()
        if missing:
            logger.warning(
                f"[fact_credit_limit_summary] Các NH không tìm thấy trong sheet "
                f"'Tổng hợp' (Credit_Limit/Limit_Type/Interest_Rate = NULL): {missing}"
            )

        # LƯU Ý: theo mapping, KHÔNG thêm dòng nào ngoài Bảng 1 "Theo dõi
        # vay NH". Khoản MB-Dài hạn 55.4 tỷ chỉ tồn tại ở sheet "Tổng hợp",
        # không có dòng nguồn tương ứng -> KHÔNG đưa vào fact này. Nếu
        # nghiệp vụ cần phản ánh, phải bổ sung mapping riêng (DA quyết định).

        # ── 3. Ép kiểu số theo Target Data Type (DECIMAL / FLOAT) ────────
        for col in ["Credit_Limit", "Granted_Limit", "Principal_Balance",
                    "Remaining_Disbursement", "Interest_Rate"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # ── 4. ID tự tăng + chuẩn hoá Bank_Code ──────────────────────────
        df = df.reset_index(drop=True)
        df["ID"] = df.index + 1
        df["Bank_Code"] = df["Bank_Code"].str.upper()

        logger.info(
            f"[fact_credit_limit_summary][OUT] {len(df)} dòng | "
            f"NH: {df['Bank_Code'].tolist()}"
        )
        return df