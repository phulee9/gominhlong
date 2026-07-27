"""Fact_CashFlow: Sổ chi tiết các tài khoản.

CẤU TRÚC FILE NGUỒN (xác nhận trên file thật: 3 sheet, 138 TK):

    Tài khoản: 111 - Tiền mặt          ← dòng header nhóm (nằm ở cột Ngày hạch toán)
        │ Số dư đầu kỳ │ … │ Dư Nợ │ Dư Có │   ← KHÔNG có số chứng từ
        │ 01/01 │ NVK0210 │ … │              ← dòng giao dịch (CÓ số chứng từ)
        │ Cộng │ … │ tổng PS Nợ/Có │          ← dòng tổng nhóm, KHÔNG có số chứng từ
    Tài khoản: 11212 - TK 114000046412 …
        │ Số dư đầu kỳ │ … │ 12000000 │ 0 │   ← TK KHÔNG phát sinh: chỉ có đúng dòng này
    …
    Tổng cộng                                 ← dòng tổng toàn sổ (cuối mỗi sheet)

QUY TẮC GIỮ DÒNG:
  • TK CÓ phát sinh    → chỉ giữ các dòng giao dịch; BỎ 'Số dư đầu kỳ' và 'Cộng'.
  • TK KHÔNG phát sinh → giữ ĐÚNG 1 dòng 'Số dư đầu kỳ' (trống hết, chỉ có Dư Nợ
    hoặc Dư Có).
  • Luôn bỏ: dòng header nhóm, dòng 'Cộng', dòng 'Tổng cộng'.

Kết quả đúng: 123 TK có phát sinh → 228.629 dòng giao dịch; 15 TK không phát
sinh → 15 dòng số dư đầu kỳ. Tổng 228.644 dòng / 138 TK.

── VÌ SAO KHÔNG DỰA VÀO CHỮ "Số dư đầu kỳ" ─────────────────────────────────
Chữ này nằm ở cột 'Diễn giải' của file gốc, NHƯNG pipeline map cột trước khi
gọi transformer và schema chỉ có 1 cột Description → cột 'Diễn giải' có thể bị
loại, marker biến mất, nhận diện theo chữ trả về 0 dòng (đã xảy ra thật).

Vì vậy nhận diện theo CẤU TRÚC, luôn đúng dù cột Diễn giải còn hay mất:
  dòng số dư đầu kỳ = KHÔNG có số chứng từ  +  KHÔNG phải dòng header
                      +  có giá trị ở cột Dư Nợ / Dư Có
Với TK không phát sinh thì MISA không sinh dòng 'Cộng' (đã kiểm chứng: cả 15
TK đều chỉ có đúng 1 dòng số dư), nên quy tắc này không thể bắt nhầm dòng tổng.
Nếu cột Diễn giải vẫn còn thì marker chữ được dùng thêm để chắc chắn.

── VÌ SAO PHẢI GÁN Posting_Date CHO DÒNG SỐ DƯ ─────────────────────────────
File MISA để trống Ngày hạch toán ở dòng số dư đầu kỳ → vào DB là NULL. Pipeline
xóa dữ liệu cũ bằng 'WHERE Posting_Date >= … AND <= …', mà NULL không khớp bất
kỳ so sánh nào → các dòng này không bao giờ bị xóa và bị cộng dồn sau MỖI lần
chạy. Nên gán Posting_Date = ngày đầu kỳ (ngày nhỏ nhất trong dữ liệu).

── PHÂN BIỆT 2 LOẠI DÒNG Ở TẦNG BI ─────────────────────────────────────────
    WHERE Voucher_No IS NOT NULL   -- phát sinh trong kỳ
    WHERE Voucher_No IS NULL       -- số dư đầu kỳ của TK không phát sinh
"""

import re

import numpy as np
import pandas as pd

from .base import BaseTransformer, TransformContext

# "Tài khoản: 1111 - Tiền mặt" → 1111
ACCOUNT_HEADER_RE = re.compile(r"tài khoản:\s*([a-z0-9_]+)", re.IGNORECASE)

# Cột số dư: khớp cả tên đã đổi (Debit_Balance/Credit_Balance) lẫn tên gốc.
BALANCE_COL_RE = re.compile(r"balance|dư\s*(nợ|có)", re.IGNORECASE)

OPENING_MARKERS = {"số dư đầu kỳ", "số dư đầu kì"}
TOTAL_MARKERS = {"cộng", "tổng cộng", "cộng phát sinh", "số dư cuối kỳ"}


def _norm_text(col: pd.Series) -> pd.Series:
    """Chuẩn hóa chuỗi: bỏ non-breaking space, gộp khoảng trắng, lower."""
    return (
        col.astype(str)
        .str.replace("\xa0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .str.lower()
    )


class FactCashFlowTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        df = df.copy()

        posting_text = _norm_text(df["Posting_Date"])
        is_header = posting_text.str.startswith("tài khoản")

        print(
            f"[fact_cashflow][IN ] {len(df)} dòng | cột: {list(df.columns)} "
            f"| header 'Tài khoản:': {int(is_header.sum())} "
            f"| không có Số chứng từ: {int(df['Voucher_No'].isna().sum())}"
        )

        # ── 1. Bóc mã tài khoản từ dòng header, forward-fill xuống các dòng con
        def extract_account(val):
            s = str(val).strip()
            if s.lower().startswith("tài khoản"):
                m = ACCOUNT_HEADER_RE.search(s)
                if m:
                    return m.group(1).upper()
            return np.nan

        df["Account_No"] = df["Posting_Date"].apply(extract_account).ffill()

        # ── 2. Dòng giao dịch = có Số chứng từ hợp lệ
        voucher = df["Voucher_No"].astype(str).str.strip()
        has_voucher = (
            df["Voucher_No"].notna()
            & voucher.ne("")
            & ~voucher.str.lower().isin({"nan", "none", "nat"})
        )

        # ── 3. Nhận diện dòng SỐ DƯ ĐẦU KỲ theo CẤU TRÚC (xem chú thích đầu file)
        balance_cols = [c for c in df.columns if BALANCE_COL_RE.search(str(c))]
        has_balance = pd.Series(False, index=df.index)
        for c in balance_cols:
            s = _norm_text(df[c])
            has_balance |= df[c].notna() & s.ne("") & ~s.isin({"nan", "none", "nat"})

        is_opening = ~has_voucher & ~is_header & has_balance
        is_total = pd.Series(False, index=df.index)

        # Nếu cột chữ (Diễn giải) còn sống thì dùng thêm marker cho chắc chắn.
        for c in df.columns:
            s = _norm_text(df[c])
            is_opening |= s.isin(OPENING_MARKERS) & ~has_voucher & ~is_header
            is_total |= s.isin(TOTAL_MARKERS) & ~has_voucher
        is_opening &= ~is_total

        if not balance_cols:
            print(
                "[fact_cashflow][WARN] không tìm thấy cột Dư Nợ/Dư Có "
                f"trong {list(df.columns)} — dòng số dư đầu kỳ có thể bị bỏ sót."
            )

        # ── 4. TK nào có phát sinh? (tính trên toàn bộ df được truyền vào)
        acc_has_txn = (
            has_voucher.groupby(df["Account_No"], dropna=False)
            .transform("any")
            .fillna(False)
            .astype(bool)
        )

        # TK không phát sinh → giữ dòng số dư đầu kỳ; TK có phát sinh → bỏ.
        keep_opening = is_opening & ~acc_has_txn & df["Account_No"].notna()
        keep = (has_voucher | keep_opening) & ~is_header & ~is_total

        out = df[keep].copy()
        out["_is_opening"] = ~has_voucher[keep].to_numpy()

        # Mỗi TK không phát sinh chỉ giữ 1 dòng số dư.
        dup_opening = out["_is_opening"] & out.duplicated(
            subset=["Account_No", "_is_opening"], keep="first"
        )
        out = out[~dup_opening]

        # ── 5. Gán Posting_Date cho dòng số dư đầu kỳ (xem chú thích đầu file).
        #      Gán đúng kiểu của cột: datetime nếu pipeline đã parse, ngược lại chuỗi.
        posting = pd.to_datetime(out["Posting_Date"], errors="coerce")
        period_start = posting.min()
        if pd.notna(period_start) and out["_is_opening"].any():
            if pd.api.types.is_datetime64_any_dtype(out["Posting_Date"]):
                fill_value = period_start
            else:
                fill_value = period_start.strftime("%Y-%m-%d %H:%M:%S")
            out.loc[out["_is_opening"], "Posting_Date"] = fill_value

        out = out.drop(columns="_is_opening")

        # ── 6. ID tự tăng
        out = out.reset_index(drop=True)
        out["ID"] = out.index + 1

        print(
            f"[fact_cashflow][OUT] {len(out)} dòng "
            f"| số dư đầu kỳ giữ lại: {int(out['Voucher_No'].isna().sum())} "
            f"| số TK: {out['Account_No'].nunique()} "
            f"| Account_No rỗng: {int(out['Account_No'].isna().sum())}"
        )

        return out