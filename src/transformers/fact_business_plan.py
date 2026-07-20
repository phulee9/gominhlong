"""Fact_BusinessPlan: Kế hoạch kinh doanh — unpivot T1..T12 thành dòng theo tháng.

2 sheet:
  S1 (KQKD_pa1):
    - Header dòng 5: TT | Khoản mục | Mã số | NĂM 2026 | T1..T12
    - Chỉ lấy dòng CÓ Mã số (cột C không rỗng)
    - Indicator_Code = "B02-DN_{mã số dạng int}" (vd: 10.0 → "B02-DN_10")

  S2 (CĐKT_pa1):
    - Header dòng 1: cột A = mã số, cột B = tên, cột C = Mã số (sai thứ tự)
    - Dùng cột A (cột đầu tiên) làm Indicator_Code, KHÔNG dùng cột C
    - Chỉ lấy dòng có ít nhất 1 giá trị T1..T12 không rỗng
    - Indicator_Code = "B01-DN_{cột A dạng int}" (vd: 100.0 → "B01-DN_100")
"""
import logging
import re

import pandas as pd

from .base import BaseTransformer, TransformContext

logger = logging.getLogger(__name__)

YEAR = 2026
MONTH_PATTERN = re.compile(r'^T(\d{1,2})$', re.IGNORECASE)


def _month_cols(columns):
    """Trả về list tên cột T1..T12 theo đúng thứ tự số tháng."""
    found = []
    for c in columns:
        m = MONTH_PATTERN.match(str(c).strip())
        if m:
            found.append((int(m.group(1)), c))
    found.sort(key=lambda x: x[0])
    return [c for _, c in found]


def _to_date(month_num: int) -> str:
    return f"{YEAR}-{month_num:02d}-01"


def _melt(df: pd.DataFrame, id_col: str, month_cols: list,
          annual_col: str = None) -> pd.DataFrame:
    """Unpivot các cột tháng thành dòng.

    Với các dòng KHÔNG có dữ liệu cả 12 tháng:
      - Nếu annual_col có giá trị → chia đều cho 12 tháng
      - Nếu annual_col cũng không có → fill 0
    Với các dòng đã có ít nhất 1 tháng có số liệu → giữ nguyên.
    """
    # Chuẩn hóa cột tháng về numeric trước khi xử lý
    for c in month_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Xác định dòng nào KHÔNG có dữ liệu tháng nào
    has_monthly = df[month_cols].notna().any(axis=1)

    if annual_col and annual_col in df.columns:
        annual_vals = pd.to_numeric(df[annual_col], errors="coerce")

        # Dòng không có tháng: fill từ NĂM 2026 / 12, hoặc 0
        for col in month_cols:
            df.loc[~has_monthly, col] = df.loc[~has_monthly].apply(
                lambda row: (annual_vals[row.name] / 12)
                if pd.notna(annual_vals[row.name])
                else 0.0,
                axis=1
            )
    else:
        # Không có cột năm → fill 0 cho tất cả dòng thiếu
        for col in month_cols:
            df.loc[~has_monthly, col] = df.loc[~has_monthly, col].fillna(0)

    df_m = pd.melt(
        df,
        id_vars=[id_col],
        value_vars=month_cols,
        var_name="Month_Str",
        value_name="Target_Amount",
    )
    month_num_map = {c: int(MONTH_PATTERN.match(str(c).strip()).group(1)) for c in month_cols}
    df_m["Month"] = pd.to_datetime(
        df_m["Month_Str"].map(month_num_map).apply(_to_date)
    ).dt.date
    df_m["Target_Amount"] = pd.to_numeric(df_m["Target_Amount"], errors="coerce").fillna(0)
    return df_m.drop(columns=["Month_Str"])


class FactBusinessPlanTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:
        sheet = str(ctx.sheet).strip()

        # ── S1: KQKD_pa1 ─────────────────────────────────────────────────
        if "KQKD" in sheet:
            df_raw = ctx.df_raw.copy()
            df_raw.columns = [str(c).strip() for c in df_raw.columns]

            # Tìm cột Mã số và các cột T1..T12
            ma_so_col = None
            for c in df_raw.columns:
                if str(c).strip().lower() == "mã số":
                    ma_so_col = c
                    break

            if not ma_so_col:
                logger.error("[S1] Không tìm thấy cột 'Mã số'")
                return pd.DataFrame(columns=["Indicator_Code", "Month", "Target_Amount"])

            mcols = _month_cols(df_raw.columns)
            if not mcols:
                logger.error("[S1] Không tìm thấy cột T1..T12")
                return pd.DataFrame(columns=["Indicator_Code", "Month", "Target_Amount"])

            # Chỉ lấy dòng CÓ Mã số (không rỗng, không phải header)
            df_s1 = df_raw[[ma_so_col] + mcols].copy()
            df_s1 = df_s1[df_s1[ma_so_col].notna()]
            df_s1 = df_s1[df_s1[ma_so_col].astype(str).str.strip().str.lower() != "mã số"]  # bỏ header lặp

            # Tạo Indicator_Code: B02-DN_{mã số dạng int}
            def to_code_s1(v):
                try:
                    return f"B02-DN_{int(float(str(v).strip()))}"
                except (ValueError, TypeError):
                    return None

            df_s1["Indicator_Code"] = df_s1[ma_so_col].apply(to_code_s1)
            df_s1 = df_s1[df_s1["Indicator_Code"].notna()]

            # Tìm cột NĂM 2026
            annual_col = None
            for c in df_raw.columns:
                if "2026" in str(c).strip() or str(c).strip().upper() == "NĂM 2026":
                    annual_col = c
                    break

            df_melted = _melt(df_s1, "Indicator_Code", mcols, annual_col=annual_col)
            logger.info("[S1-KQKD] %d dòng sau melt (%d chỉ tiêu x 12 tháng)",
                        len(df_melted), len(df_s1))
            return df_melted[["Indicator_Code", "Month", "Target_Amount"]]

        # ── S2: CĐKT_pa1 ─────────────────────────────────────────────────
        if "CĐKT" in sheet or "CDKT" in sheet:
            df_raw = ctx.df_raw.copy()
            df_raw.columns = [str(c).strip() for c in df_raw.columns]

            # Dùng cột đầu tiên (cột A) làm Indicator_Code — cột "Mã số" bị lệch số
            col_a = df_raw.columns[0]
            mcols = _month_cols(df_raw.columns)
            if not mcols:
                logger.error("[S2] Không tìm thấy cột T1..T12")
                return pd.DataFrame(columns=["Indicator_Code", "Month", "Target_Amount"])

            # Tìm cột NĂM 2026
            annual_col = None
            for c in df_raw.columns:
                if "2026" in str(c).strip() or str(c).strip().upper() in ("NĂM 2026", "NAM 2026"):
                    annual_col = c
                    break

            df_s2 = df_raw[[col_a] + ([annual_col] if annual_col else []) + mcols].copy()

            # Lọc dòng có cột A hợp lệ
            df_s2 = df_s2[df_s2[col_a].notna()]
            df_s2 = df_s2[df_s2[col_a].astype(str).str.strip() != ""]

            # FIX: format int(float(v)) để 110.0 → "B01-DN_110", không phải "B01-DN_110.0"
            def to_code_s2(v):
                v_str = str(v).strip()
                if not v_str or v_str in ("nan", "None", ""):
                    return None
                # Thử convert số float → int (110.0 → "110")
                try:
                    return f"B01-DN_{int(float(v_str))}"
                except (ValueError, TypeError):
                    # Giữ nguyên string có chữ: "411a", "421b"...
                    return f"B01-DN_{v_str}"

            df_s2["Indicator_Code"] = df_s2[col_a].apply(to_code_s2)
            df_s2 = df_s2[df_s2["Indicator_Code"].notna()]

            df_melted = _melt(df_s2, "Indicator_Code", mcols, annual_col=annual_col)
            logger.info("[S2-CĐKT] %d dòng sau melt (%d chỉ tiêu x 12 tháng)",
                        len(df_melted), len(df_s2))
            return df_melted[["Indicator_Code", "Month", "Target_Amount"]]
        # ── S3: Target_Vong_Quay ──────────────────────────────────────────
        if "Vong_Quay" in sheet or "Vong_quay" in sheet or "VONG_QUAY" in sheet.upper():
            df_raw = ctx.df_raw.copy()
            df_raw.columns = [str(c).strip() for c in df_raw.columns]

            # Cột đầu tiên = Mã, giữ nguyên không thêm tiền tố
            col_ma = df_raw.columns[0]

            # Tìm cột NĂM 2026
            annual_col = None
            for c in df_raw.columns:
                if "2026" in str(c).strip():
                    annual_col = c
                    break

            if not annual_col:
                logger.error("[S3] Không tìm thấy cột NĂM 2026")
                return pd.DataFrame(columns=["Indicator_Code", "Month", "Target_Amount"])

            df_s3 = df_raw[[col_ma, annual_col]].copy()

            # Lọc dòng hợp lệ: có mã, không rỗng
            df_s3 = df_s3[df_s3[col_ma].notna()]
            df_s3 = df_s3[df_s3[col_ma].astype(str).str.strip() != ""]
            df_s3 = df_s3[df_s3[col_ma].astype(str).str.strip().str.lower() != "mã"]  # bỏ header lặp

            df_s3["Indicator_Code"] = df_s3[col_ma].astype(str).str.strip()
            df_s3 = df_s3[df_s3["Indicator_Code"].notna() & (df_s3["Indicator_Code"] != "")]

            # Chia đều NĂM 2026 / 12 cho từng tháng
            df_s3["_annual"] = pd.to_numeric(df_s3[annual_col], errors="coerce").fillna(0)

            rows = []
            for _, row in df_s3.iterrows():
                monthly_val = row["_annual"] / 12
                for month_num in range(1, 13):
                    rows.append({
                        "Indicator_Code": row["Indicator_Code"],
                        "Month": pd.to_datetime(_to_date(month_num)).date(),
                        "Target_Amount": round(monthly_val, 6),
                    })

            df_melted = pd.DataFrame(rows, columns=["Indicator_Code", "Month", "Target_Amount"])
            logger.info("[S3-VongQuay] %d dòng sau expand (%d chỉ tiêu x 12 tháng)",
                        len(df_melted), len(df_s3))
            return df_melted[["Indicator_Code", "Month", "Target_Amount"]]