"""Fact_AccountsReceivable: Chi tiết công nợ phải thu khách hàng.

Quy tắc lọc dòng dùng chung ở base_debt.BaseDebtTransformer:
  • KH có phát sinh    → chỉ giữ dòng giao dịch (bỏ 'Số dư đầu kỳ' và 'Cộng').
  • KH không phát sinh → giữ 1 dòng 'Số dư đầu kỳ' (chỉ có Dư Nợ/Dư Có).
  • Bỏ header nhóm 'Tên khách hàng:', dòng 'Cộng', 'Tổng cộng'.
  • Dòng số dư đầu kỳ được gán Posting_Date = đầu kỳ (tránh trùng khi reload).

Kiểm chứng trên file thật: 26.173 → 23.193 dòng (22.698 giao dịch + 495 số dư).
Phân biệt ở BI: Voucher_No IS NOT NULL = phát sinh; IS NULL = số dư đầu kỳ.
"""

from .base_debt import BaseDebtTransformer


class FactAccountsReceivableTransformer(BaseDebtTransformer):
    GROUP_HEADER_PREFIX = "tên khách hàng:"
    LABEL = "fact_accounts_receivable"