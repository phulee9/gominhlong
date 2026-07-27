"""Logic lọc dòng CHUNG cho fact công nợ (phải thu / phải trả).

Hai transformer (fact_accounts_receivable, fact_accounts_payable) dùng chung
lớp này; mỗi lớp con chỉ khác chữ header nhóm ('Tên khách hàng:' vs
'Tên nhà cung cấp:'). Đặt cạnh base.py trong cùng package transformer.

CẤU TRÚC FILE NGUỒN (xác nhận trên file thật, cả 2 file 1 sheet):

    Tên khách hàng: A Diện …            ← header nhóm, nằm ở cột Partner_Code
      NTQV03 │ … │ Số dư đầu kỳ │ … │ Dư Nợ │   ← KH không phát sinh: chỉ số dư
      NTQV03 │ … │ Cộng         │ … │          ← dòng tổng nhóm
    Tên khách hàng: A Hoàng Hoan …
      NTHH14 │ … │ Số dư đầu kỳ │ …             ← có số dư đầu kỳ
      NTHH14 │ ngày │ BH00729 │ giao dịch │     ← dòng giao dịch (CÓ số chứng từ)
      NTHH14 │ … │ Cộng                         ← dòng tổng nhóm
    …
    Tổng cộng                                   ← dòng tổng toàn sổ

QUY TẮC GIỮ DÒNG (giống Fact_CashFlow):
  • Đối tượng CÓ phát sinh    → chỉ giữ dòng giao dịch; BỎ 'Số dư đầu kỳ' và 'Cộng'.
  • Đối tượng KHÔNG phát sinh → giữ ĐÚNG 1 dòng 'Số dư đầu kỳ' (chỉ có dư nợ/dư có).
  • Luôn bỏ: dòng header nhóm, dòng 'Cộng', dòng 'Tổng cộng'.

ĐẶC ĐIỂM (khác Fact_CashFlow):
  • Header nhóm nằm ở cột Partner_Code (không phải Posting_Date); mã đối tượng
    đã có SẴN ở từng dòng con → KHÔNG cần forward-fill.
  • Cột 'Diễn giải' (chứa marker) được config GIỮ LẠI (map → Description) nên
    marker chữ dùng được; vẫn kèm nhận diện theo cấu trúc để an toàn.

PHÂN BIỆT Ở TẦNG BI:
    WHERE Voucher_No IS NOT NULL  -- phát sinh trong kỳ
    WHERE Voucher_No IS NULL      -- số dư đầu kỳ của đối tượng không phát sinh

GÁN Posting_Date CHO DÒNG SỐ DƯ: file để trống Ngày hạch toán ở dòng số dư →
NULL không khớp 'WHERE Posting_Date BETWEEN …' của bước xóa → trùng dữ liệu sau
mỗi lần chạy. Nên gán = ngày nhỏ nhất trong dữ liệu (đầu kỳ).
"""

import pandas as pd

from .base import BaseTransformer, TransformContext

OPENING_MARKERS = {"số dư đầu kỳ", "số dư đầu kì"}
TOTAL_MARKERS = {"cộng", "cong", "tổng cộng", "tong cong", "số dư cuối kỳ"}

MONEY_COLS = ["Debit_Amount", "Credit_Amount", "Ending_Debit_Balance", "Ending_Credit_Balance"]
BALANCE_COLS = ["Ending_Debit_Balance", "Ending_Credit_Balance"]


def _norm(col: pd.Series) -> pd.Series:
    return (
        col.astype(str)
        .str.replace("\xa0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .str.lower()
    )


class BaseDebtTransformer(BaseTransformer):
    """Lớp con PHẢI set 2 thuộc tính: GROUP_HEADER_PREFIX và LABEL."""

    GROUP_HEADER_PREFIX = ""      # ví dụ "tên khách hàng:"
    LABEL = "debt"

    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        df = df.copy()

        pc = df["Partner_Code"] if "Partner_Code" in df.columns else pd.Series("", index=df.index)
        pc_norm = _norm(pc)
        is_header = pc_norm.str.startswith(self.GROUP_HEADER_PREFIX)

        print(
            f"[{self.LABEL}][IN ] {len(df)} dòng | cột: {list(df.columns)} "
            f"| header nhóm: {int(is_header.sum())} "
            f"| không có Số chứng từ: {int(df['Voucher_No'].isna().sum()) if 'Voucher_No' in df.columns else 'N/A'}"
        )

        # Dòng giao dịch = có Số chứng từ hợp lệ
        if "Voucher_No" in df.columns:
            v = df["Voucher_No"].astype(str).str.strip()
            has_voucher = (
                df["Voucher_No"].notna()
                & v.ne("")
                & ~v.str.lower().isin({"nan", "none", "nat"})
            )
        else:
            has_voucher = pd.Series(False, index=df.index)

        desc_norm = _norm(df["Description"]) if "Description" in df.columns else pd.Series("", index=df.index)
        is_total = desc_norm.isin(TOTAL_MARKERS) & ~has_voucher

        # 'Số dư đầu kỳ': marker chữ HOẶC cấu trúc (không số chứng từ + không
        # header + không phải dòng Cộng + có giá trị dư).
        present_balance = [c for c in BALANCE_COLS if c in df.columns]
        has_balance = pd.Series(False, index=df.index)
        for c in present_balance:
            s = _norm(df[c])
            has_balance |= df[c].notna() & s.ne("") & ~s.isin({"nan", "none", "nat", "0"})

        is_opening = (
            (desc_norm.isin(OPENING_MARKERS) | (~has_voucher & ~is_header & ~is_total & has_balance))
            & ~is_header
            & ~is_total
        )

        # Đối tượng nào có phát sinh?
        acc_has_txn = (
            has_voucher.groupby(pc.where(~is_header), dropna=True).transform("any")
        )
        acc_has_txn = acc_has_txn.reindex(df.index).fillna(False).astype(bool)

        keep_opening = is_opening & ~acc_has_txn & pc.ne("") & pc.notna() & ~is_header
        keep = (has_voucher | keep_opening) & ~is_header & ~is_total

        out = df[keep].copy()
        out["_is_opening"] = ~has_voucher[keep].to_numpy()

        # Mỗi đối tượng không phát sinh chỉ giữ 1 dòng số dư.
        dup = out["_is_opening"] & out.duplicated(subset=["Partner_Code", "_is_opening"], keep="first")
        out = out[~dup]

        # Gán Posting_Date cho dòng số dư đầu kỳ (xem chú thích đầu file).
        if "Posting_Date" in out.columns:
            posting = pd.to_datetime(out["Posting_Date"], errors="coerce")
            period_start = posting.min()
            if pd.notna(period_start) and out["_is_opening"].any():
                if pd.api.types.is_datetime64_any_dtype(out["Posting_Date"]):
                    fill = period_start
                else:
                    fill = period_start.strftime("%Y-%m-%d %H:%M:%S")
                out.loc[out["_is_opening"], "Posting_Date"] = fill

        out = out.drop(columns="_is_opening")

        # Ép kiểu số cho cột tiền
        for col in MONEY_COLS:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

        # ID tự tăng
        out = out.reset_index(drop=True)
        out["ID"] = out.index + 1

        print(
            f"[{self.LABEL}][OUT] {len(out)} dòng "
            f"| số dư đầu kỳ giữ lại: {int(out['Voucher_No'].isna().sum()) if 'Voucher_No' in out.columns else 'N/A'} "
            f"| số đối tượng: {out['Partner_Code'].nunique()}"
        )

        return out