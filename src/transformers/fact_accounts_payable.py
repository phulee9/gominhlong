"""Fact_AccountsPayable: Chi tiết công nợ phải trả nhà cung cấp.

Quy tắc lọc dòng dùng chung ở base_debt.BaseDebtTransformer:
  • NCC có phát sinh    → chỉ giữ dòng giao dịch (bỏ 'Số dư đầu kỳ' và 'Cộng').
  • NCC không phát sinh → giữ 1 dòng 'Số dư đầu kỳ' (chỉ có Dư Nợ/Dư Có).
  • Bỏ header nhóm 'Tên nhà cung cấp:', dòng 'Cộng', 'Tổng cộng'.
  • Dòng số dư đầu kỳ được gán Posting_Date = đầu kỳ (tránh trùng khi reload).

Kiểm chứng trên file thật: 17.048 → 15.412 dòng (15.293 giao dịch + 119 số dư).
Phân biệt ở BI: Voucher_No IS NOT NULL = phát sinh; IS NULL = số dư đầu kỳ.
"""

from .base_debt import BaseDebtTransformer


class FactAccountsPayableTransformer(BaseDebtTransformer):
    GROUP_HEADER_PREFIX = "tên nhà cung cấp:"
    LABEL = "fact_accounts_payable"