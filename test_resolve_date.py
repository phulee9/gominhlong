# test_resolve_date.py
import sys
sys.path.insert(0, "/mnt/c/excel-pipeline")
from src.dags.excel_pipeline_dag import _resolve_file_date

# Test file có hậu tố tháng
print(_resolve_file_date("fact_balance_sheet", "data/raw/misa/B01_DN_Bao_cao_tinh_hinh_tai_chinh_2026-06.xlsx"))
# Kỳ vọng: 2026-06-30

print(_resolve_file_date("fact_income_statement", "data/raw/misa/B02_DN_Bao_cao_ket_qua_hoat_dong_kinh_doanh_2026-07.xlsx"))
# Kỳ vọng: 2026-07-31

# Test file KHÔNG có hậu tố tháng (báo cáo còn lại)
print(_resolve_file_date("fact_accounts_receivable", "data/raw/misa/Chi_tiet_cong_no_phai_thu_khach_hang.xlsx"))
# Kỳ vọng: fallback bookmark (hoặc None nếu chưa set Variable)

# Test file nội bộ Google Sheets
print(_resolve_file_date("fact_loan", "data/raw/noibo/bc_tin_dung_2026.xlsx"))
# Kỳ vọng: None