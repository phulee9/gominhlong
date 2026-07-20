"""
Dim_Bank: enrich Code1 (mã BANK cụt trong HĐTG) và Code2 (bản sao Bank_Code).
Code1: map tay 7 mã cụt; dòng null -> fill từ Bank_Code.
Code2: duplicate nguyên Bank_Code.
Chạy transform TRƯỚC khi load nên df còn PascalCase 'Bank_Code'.
"""
import logging
import pandas as pd
from .base import BaseTransformer, TransformContext

logger = logging.getLogger(__name__)

_FACT_CODE_MAP = {
    "Agribank":   "AGR",
    "HDBank":     "HD",
    "LPBank":     "LP",
    "VPBank":     "VP",
    "Woori Bank": "Woori",
    "IVB":        "IVB",
    "MB":         "MB",
    "TPBank": "TP"
}

class DimBankTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        df["Code1"] = df["Bank_Code"].map(_FACT_CODE_MAP)
        df["Code1"] = df["Code1"].fillna(df["Bank_Code"]).str.upper()   # null -> Bank_Code
        df["Code2"] = df["Bank_Code"]                        # duplicate
        
        expected = set(_FACT_CODE_MAP.keys())
        missing = expected - set(df["Bank_Code"].dropna())
        if missing:
            logger.warning(
                "[Dim_Bank] Các Bank_Code cần cho HĐTG nhưng KHÔNG có trong "
                "Danh_sach_ngan_hang.xlsx: %s.", sorted(missing),
            )
        return df