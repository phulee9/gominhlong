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
  S3 (Target_Vong_Quay):
    - Cột đầu = Mã (giữ nguyên, không thêm tiền tố), cột NĂM 2026 chia 12 tháng

QUY TẮC CHIA 12 THÁNG (fix lệch làm tròn — issue DA báo):
  Không chia annual/12 rồi round từng tháng (12 × round(x/12) ≠ x → tổng
  12 tháng lệch so với NĂM 2026). Thay vào đó:
      base  = round(annual / 12, ndigits)   → gán cho T2..T12 (11 tháng)
      T1    = annual - 11 * base            → hứng trọn phần dư
  Bảo toàn tuyệt đối: T1 + 11*base == round(annual, ndigits).

  QUY TẮC LÀM TRÒN PHẢI DÙNG ROUND_HALF_UP QUA Decimal (KHÔNG dùng
  round()/.round() mặc định của Python/pandas):
    - Python float round() / pandas Series.round() dùng round-half-to-even
      (banker's rounding): 0.5 -> 0, 1.5 -> 2, 2.5 -> 2 ...
    - MySQL / PostgreSQL / SQL Server làm tròn cột DECIMAL / hàm ROUND()
      theo round-half-away-from-zero: 0.5 -> 1, 1.5 -> 2, 2.5 -> 3 ...
    - Nếu 2 tầng (code Python và DB) dùng 2 quy tắc làm tròn khác nhau,
      những giá trị rơi đúng vào ranh giới .5 (ở vị trí ndigits) sẽ bị làm
      tròn khác nhau ở 2 tầng -> phá vỡ invariant T1+11*base == annual,
      lệch cộng dồn qua nhiều dòng x 12 tháng.
    - Dùng Decimal (không dùng float thuần) để tránh sai số biểu diễn nhị
      phân (VD 0.1, 0.15 không biểu diễn chính xác trong IEEE754 float),
      vốn có thể đẩy 1 giá trị lệch sang đúng phía ranh giới .5 sai.

  ROUND_MONEY / ROUND_RATIO PHẢI khớp đúng scale (số chữ số thập phân)
  của cột Target_Amount trong DB (DECIMAL(precision, scale)). Đây LÀ HẰNG
  SỐ DUY NHẤT cần đồng bộ thủ công với migration/schema DB — nếu schema
  đổi scale, phải sửa 2 hằng số này theo, nếu không DB sẽ round thêm lần
  nữa (double rounding) và phá vỡ bảo toàn tổng dù công thức đúng.
"""
import logging
import re
from decimal import Decimal, ROUND_HALF_UP

import numpy as np
import pandas as pd

from .base import BaseTransformer, TransformContext

logger = logging.getLogger(__name__)

YEAR = 2026
MONTH_PATTERN = re.compile(r'^T(\d{1,2})$', re.IGNORECASE)

# ⚠️ PHẢI khớp CHÍNH XÁC với scale (DECIMAL(x, scale)) của cột Target_Amount
# trong migration/schema DB. Lệch 1 đơn vị scale giữa đây và DB -> double
# rounding, phá vỡ bảo toàn tổng (xem docstring đầu file).
ROUND_MONEY = 0   # VND: làm tròn đến đồng. Nếu cột DB là DECIMAL(x, 2) thì đổi = 2
ROUND_RATIO = 2   # hệ số vòng quay (S3) — ĐÃ XÁC NHẬN qua dữ liệu thật: cột
                  # Target_Amount trong DB có scale=2. Trước đây để 6 gây double
                  # rounding (VD Vong_quay_ton_kho: annual=3.7 -> Python tính đúng
                  # 3.7 ở 6dp, nhưng DB tự round lại về 2dp (0.31+0.31x11=3.72)
                  # -> lệch 0.02). Nếu schema DB thay đổi scale, PHẢI đổi số này theo.


def _round_half_up(value, ndigits: int) -> float:
    """Làm tròn kiểu round-half-away-from-zero (khớp hành vi DECIMAL/ROUND()
    của DB), dùng Decimal để tránh sai số biểu diễn nhị phân của float.
    KHÔNG dùng round()/.round() mặc định — chúng làm tròn half-to-even,
    khác quy tắc DB dùng, gây lệch ở các giá trị rơi đúng ranh giới .5.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0.0
    quant = Decimal(1).scaleb(-ndigits)  # 10^-ndigits, vd ndigits=2 -> Decimal('0.01')
    d = Decimal(str(value))  # str() trước khi vào Decimal để giữ đúng giá trị thập phân thấy được, tránh nhiễu nhị phân của float
    return float(d.quantize(quant, rounding=ROUND_HALF_UP))


def _round_series_half_up(s: pd.Series, ndigits: int) -> pd.Series:
    """Áp dụng _round_half_up cho từng phần tử của một Series (vectorized
    round() của pandas không hỗ trợ ROUND_HALF_UP nên phải apply thủ công)."""
    return s.apply(lambda v: _round_half_up(v, ndigits))


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


def _largest_remainder_round(values: pd.Series, ndigits: int) -> pd.Series:
    """Làm tròn một TẬP giá trị (annual của nhiều dòng cùng lúc) sao cho
    TỔNG các giá trị đã làm tròn == round(TỔNG raw, ndigits) CHÍNH XÁC
    (Largest Remainder Method / Hare-Niemeyer — kỹ thuật chuẩn dùng trong
    apportionment/phân bổ ngân sách).

    LÝ DO CẦN HÀM NÀY: _split_annual làm tròn annual của TỪNG DÒNG độc lập
    (mỗi dòng round riêng để chia 12 tháng). Với 1 dòng, tổng 12 tháng luôn
    khớp đúng annual của DÒNG ĐÓ (đã đảm bảo). Nhưng khi CỘNG NHIỀU DÒNG lại
    (VD tổng toàn bảng, hoặc tổng nhóm chỉ tiêu), sai số làm tròn của từng
    dòng (tối đa ±0.5 đơn vị/dòng) cộng dồn và KHÔNG tự triệt tiêu — với
    hàng trăm dòng có annual dạng thập phân (VD 703,627,308,418.58 VNĐ do
    công thức Excel tạo ra), tổng có thể lệch vài đơn vị so với tổng gốc.

    Largest Remainder Method giải quyết bằng cách: lấy phần nguyên (floor)
    của tất cả các dòng trước, rồi phân bổ đúng phần dư còn thiếu (so với
    tổng đã làm tròn 1 LẦN DUY NHẤT ở mức tổng) cho các dòng có phần thập
    phân bị cắt bỏ LỚN NHẤT — đảm bảo tổng luôn khớp tuyệt đối, mỗi dòng
    chỉ lệch tối đa 1 đơn vị so với giá trị gốc của chính nó (không đổi
    invariant per-dòng, chỉ thay đổi CÁCH quyết định dòng nào +1/-1 đơn vị).
    """
    if len(values) == 0:
        return values
    scale = 10 ** ndigits
    scaled = values * scale
    floor_vals = np.floor(scaled)
    remainders = scaled - floor_vals
    total_target = round(scaled.sum())  # làm tròn TỔNG một lần duy nhất
    deficit = int(round(total_target - floor_vals.sum()))
    result = floor_vals.copy()
    if deficit > 0:
        # thiếu `deficit` đơn vị -> +1 cho các dòng có phần dư bị cắt lớn nhất
        top_idx = remainders.sort_values(ascending=False).index[:deficit]
        result.loc[top_idx] += 1
    elif deficit < 0:
        # thừa -> -1 cho các dòng có phần dư nhỏ nhất
        bottom_idx = remainders.sort_values(ascending=True).index[:(-deficit)]
        result.loc[bottom_idx] -= 1
    return result / scale


def _split_annual(annual: pd.Series, ndigits: int):
    """Chia annual thành 12 tháng KHÔNG lệch tổng, dùng ROUND_HALF_UP.

    base  = ROUND_HALF_UP(annual/12) → gán cho T2..T12 (11 tháng)
    first = ROUND_HALF_UP(annual - 11*base) → gán cho T1 (hứng trọn phần dư)
    Bảo toàn: first + 11*base == ROUND_HALF_UP(annual) (chính xác tuyệt đối,
    và khớp với cách DB sẽ làm tròn khi insert, nên không còn double
    rounding lệch pha giữa code và DB — miễn ROUND_MONEY/ROUND_RATIO đúng
    scale cột DB).
    """
    base = _round_series_half_up(annual / 12, ndigits)
    first = _round_series_half_up(annual - base * 11, ndigits)
    return first, base


def _melt(df: pd.DataFrame, id_col: str, month_cols: list,
          annual_col: str = None, ndigits: int = ROUND_MONEY) -> pd.DataFrame:
    """Unpivot các cột tháng thành dòng.

    Với các dòng KHÔNG có dữ liệu cả 12 tháng:
      - Nếu annual_col có giá trị → chia 12 theo quy tắc _split_annual
        (T1 hứng phần dư, T2..T12 nhận phần chia đều đã round), tổng nhiều
        dòng được đảm bảo khớp tuyệt đối bằng Largest Remainder Method.
      - Nếu annual_col cũng không có → fill 0
    Với các dòng đã có ít nhất 1 tháng có số liệu → giữ nguyên PHÂN BỔ theo
    tháng của nguồn, nhưng nếu tổng 12 tháng KHÔNG khớp annual của chính
    dòng đó (thường do chính công thức/làm tròn trong file Excel gốc gây
    ra — không phải lỗi ở code này), phần chênh lệch được gán bù vào T1,
    theo đúng quy ước "T1 hứng phần dư" dùng thống nhất trong toàn file.
    Việc này ĐẢM BẢO tổng mỗi dòng luôn khớp annual, dù dữ liệu tháng gốc
    có tự thân bị lệch hay không.
    """
    # Chuẩn hóa cột tháng về numeric trước khi xử lý
    for c in month_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Xác định dòng nào KHÔNG có dữ liệu tháng nào
    has_monthly = df[month_cols].notna().any(axis=1)

    if annual_col and annual_col in df.columns:
        annual_vals = pd.to_numeric(df[annual_col], errors="coerce").fillna(0.0)

        # LRM áp dụng 1 LẦN DUY NHẤT cho TOÀN BỘ cột annual của sheet này
        # (cả nhóm has_monthly lẫn nhóm split cùng lúc) — đây là điểm mấu
        # chốt để tổng CẢ SHEET khớp tuyệt đối round(tổng annual gốc,
        # ndigits), không chỉ khớp trong nội bộ từng nhóm con. Nếu áp dụng
        # LRM riêng cho từng nhóm, mỗi nhóm tự khớp nhưng tổng 2 nhóm cộng
        # lại vẫn có thể lệch (do mỗi nhóm làm tròn TỔNG của nó độc lập).
        annual_adjusted = _largest_remainder_round(annual_vals, ndigits)

        # (1) Nhóm ĐÃ có dữ liệu tháng: giữ nguyên phân bổ mùa vụ nhưng
        # reconcile để tổng khớp annual_adjusted — làm tròn T2..T12 trước
        # (giữ nguyên phân bổ tương đối), T1 hứng phần dư còn lại (đúng quy
        # ước T1 dùng thống nhất toàn file). Tránh cộng phần dư CHƯA làm
        # tròn vào T1 rồi round chung với các ô khác (sẽ tái tạo lại nhiễu
        # làm tròn độc lập qua nhiều ô).
        if has_monthly.any():
            t1_col = month_cols[0]
            rest_cols = month_cols[1:]
            for c in rest_cols:
                df.loc[has_monthly, c] = _round_series_half_up(
                    df.loc[has_monthly, c].fillna(0.0), ndigits
                )
            rest_sum = df.loc[has_monthly, rest_cols].sum(axis=1)
            old_t1 = df.loc[has_monthly, t1_col].fillna(0.0)
            new_t1 = annual_adjusted.loc[has_monthly] - rest_sum
            changed = (old_t1 - new_t1).abs() >= 10 ** (-ndigits) / 2
            df.loc[has_monthly, t1_col] = new_t1
            if changed.any():
                logger.info(
                    "[_melt] Reconcile %d dòng có sẵn dữ liệu tháng nhưng tự "
                    "thân không khớp annual (đã gán phần lệch vào %s)",
                    int(changed.sum()), t1_col,
                )

        # (2) Nhóm CHƯA có dữ liệu tháng: chia đều từ annual_adjusted (đã ở
        # đúng scale ndigits nên _split_annual chỉ còn làm nhiệm vụ chia 12,
        # không round thêm sai lệch nào nữa).
        first, base = _split_annual(annual_adjusted, ndigits)
        # month_cols đã sort theo số tháng → month_cols[0] chắc chắn là T1
        for i, col in enumerate(month_cols):
            fill = first if i == 0 else base
            df.loc[~has_monthly, col] = fill[~has_monthly]
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
    # Làm tròn về đúng ndigits TRƯỚC khi trả về — kể cả các dòng has_monthly
    # giữ nguyên giá trị gốc — để không còn giá trị "lửng" scale khác DB,
    # tránh double rounding khi insert (xem docstring đầu file).
    df_m["Target_Amount"] = _round_series_half_up(df_m["Target_Amount"], ndigits)
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
            if annual_col and annual_col not in df_s1.columns:
                df_s1[annual_col] = df_raw.loc[df_s1.index, annual_col]

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

            # format int(float(v)) để 110.0 → "B01-DN_110", không phải "B01-DN_110.0"
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

            # Chia NĂM 2026 thành 12 tháng: T1 hứng phần dư, T2..T12 chia đều
            # (dùng chung _round_half_up với S1/S2 — không tự viết lại logic
            # làm tròn riêng để tránh lệch pha giữa các sheet).
            # Áp dụng Largest Remainder Method trên TOÀN BỘ cột annual của
            # sheet này trước, để tổng annual sau làm tròn khớp tuyệt đối
            # tổng annual gốc (tránh lệch cộng dồn qua nhiều mã chỉ tiêu —
            # xem docstring _largest_remainder_round).
            annual_vals_s3 = pd.to_numeric(df_s3[annual_col], errors="coerce").fillna(0.0)
            df_s3["_annual"] = _largest_remainder_round(annual_vals_s3, ROUND_RATIO)
            rows = []
            for _, row in df_s3.iterrows():
                base = _round_half_up(row["_annual"] / 12, ROUND_RATIO)
                first = _round_half_up(row["_annual"] - base * 11, ROUND_RATIO)
                for month_num in range(1, 13):
                    rows.append({
                        "Indicator_Code": row["Indicator_Code"],
                        "Month": pd.to_datetime(_to_date(month_num)).date(),
                        "Target_Amount": first if month_num == 1 else base,
                    })
            df_melted = pd.DataFrame(rows, columns=["Indicator_Code", "Month", "Target_Amount"])
            logger.info("[S3-VongQuay] %d dòng sau expand (%d chỉ tiêu x 12 tháng)",
                        len(df_melted), len(df_s3))
            return df_melted[["Indicator_Code", "Month", "Target_Amount"]]