"""
__init__.py - Registry các transformer, tra theo `transformer_class` trong pipeline_config.yaml.

Mỗi sheet trong config khai báo:
    transformer_class: "dim_partner"

ExcelReader sẽ gọi get_transformer("dim_partner") để lấy instance tương ứng
và chạy .transform(df, ctx).

Bảng nào KHÔNG có transformer_class (vd: Dim_Bank, Dim_Warehouse) thì không
cần entry ở đây — ExcelReader bỏ qua bước transform, dùng df sau field_mapping luôn.
"""
from typing import Dict, Type

from .base import BaseTransformer, TransformContext

from .dim_partner import DimPartnerTransformer
from .dim_account import DimAccountTransformer
from .dim_account_number import DimAccountNumberTransformer
from .dim_product import DimProductTransformer
from .dim_report_item import DimReportItemTransformer
from .fact_inventory_outward import FactInventoryOutwardTransformer
from .fact_inventory_inward import FactInventoryInwardTransformer
from .fact_inventory_balance import FactInventoryBalanceTransformer
from .fact_cashflow import FactCashFlowTransformer
from .fact_income_statement import FactIncomeStatementTransformer
from .fact_business_plan import FactBusinessPlanTransformer
from .fact_balance_sheet import FactBalanceSheetTransformer
from .fact_term_deposit import FactTermDepositTransformer
from .fact_accounts_receivable import FactAccountsReceivableTransformer
from .fact_accounts_payable import FactAccountsPayableTransformer
from .fact_credit_limit_summary import FactCreditLimitSummaryTransformer
from .fact_loan import FactLoanTransformer
from .fact_collateral import FactCollateralTransformer

_REGISTRY: Dict[str, Type[BaseTransformer]] = {
    "dim_partner": DimPartnerTransformer,
    "dim_account": DimAccountTransformer,
    "dim_account_number": DimAccountNumberTransformer,
    "dim_product": DimProductTransformer,
    "dim_report_item": DimReportItemTransformer,
    "fact_inventory_outward": FactInventoryOutwardTransformer,
    "fact_inventory_inward": FactInventoryInwardTransformer,
    "fact_inventory_balance": FactInventoryBalanceTransformer,
    "fact_cashflow": FactCashFlowTransformer,
    "fact_income_statement": FactIncomeStatementTransformer,
    "fact_business_plan": FactBusinessPlanTransformer,
    "fact_balance_sheet": FactBalanceSheetTransformer,
    "fact_term_deposit": FactTermDepositTransformer,
    "fact_accounts_receivable": FactAccountsReceivableTransformer,
    "fact_accounts_payable": FactAccountsPayableTransformer,
    "fact_credit_limit_summary": FactCreditLimitSummaryTransformer,
    "fact_loan": FactLoanTransformer,
    "fact_collateral": FactCollateralTransformer,
}


def get_transformer(name: str) -> BaseTransformer:
    cls = _REGISTRY.get(name)
    if cls is None:
        raise ValueError(
            f"Không tìm thấy transformer '{name}'. "
            f"Các transformer đã đăng ký: {sorted(_REGISTRY)}"
        )
    return cls()


def available_transformers() -> list:
    return sorted(_REGISTRY)


__all__ = ["BaseTransformer", "TransformContext", "get_transformer", "available_transformers"]
