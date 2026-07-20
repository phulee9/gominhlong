from .base import BaseTransformer, TransformContext
import pandas as pd


class DimPartnerTransformer(BaseTransformer):

    def transform(self, df: pd.DataFrame, ctx: TransformContext) -> pd.DataFrame:

        # ── BATCH 1: Khách hàng → load thẳng ────────────────────────────
        if "khach_hang" in ctx.file_id:
            df["Partner_Group"] = "Khách hàng"
            return df

        # ── BATCH 2: Nhà cung cấp → dedup với KH đã có trong DB ─────────
        if "nha_cung_cap" in ctx.file_id:
            df["Partner_Group"] = "Nhà cung cấp"

            # Lookup toàn bộ KH đã load ở batch 1 từ Database
            df_kh = ctx.lookup("Dim_Partner") if ctx.lookup else pd.DataFrame()

            if df_kh.empty:
                # Dry-run hoặc chưa có DB → load NCC thẳng, không dedup
                return df

            # Chuẩn hóa tên cột dữ liệu lịch sử từ DB về chữ thường để xử lý
            df_kh.columns = [c.lower() for c in df_kh.columns]

            # Chỉ giữ các cột cần thiết từ nhóm Khách hàng
            KH_COLS = ["partner_code", "partner_name", "address", "tax", "partner_group"]
            df_kh = df_kh[[c for c in KH_COLS if c in df_kh.columns]].copy()
            
            # Đổi tên cột về dạng CamelCase giống với cấu trúc bảng df đầu vào từ MISA
            df_kh = df_kh.rename(columns={
                "partner_code"  : "Partner_Code",
                "partner_name"  : "Partner_Name",
                "address"       : "Address",
                "tax"           : "Tax",
                "partner_group" : "Partner_Group",
            })

            # TẠO CỘT PHỤ CHUẨN HÓA: Xóa khoảng trắng + Chuyển chữ hoa toàn bộ để so sánh chính xác
            df["Partner_Code_Clean"] = df["Partner_Code"].astype(str).str.strip().str.upper()
            df_kh["Partner_Code_Clean"] = df_kh["Partner_Code"].astype(str).str.strip().str.upper()

            # Tìm tập hợp các mã đối tác trùng nhau giữa Khách hàng và Nhà cung cấp
            kh_codes   = set(df_kh["Partner_Code_Clean"])
            ncc_codes  = set(df["Partner_Code_Clean"])
            codes_trung = kh_codes.intersection(ncc_codes)

            # TÁCH DỮ LIỆU THÀNH 3 NHÓM ĐỂ XỬ LÝ DEDUP:
            
            # Nhóm 1: Khách hàng thuần túy (Chỉ có ở batch 1, không bị trùng với NCC)
            df_kh_pure = df_kh[~df_kh["Partner_Code_Clean"].isin(codes_trung)].copy()

            # Nhóm 2: Nhà cung cấp thuần túy (Chỉ có ở batch 2, không bị trùng với KH)
            df_ncc_pure = df[~df["Partner_Code_Clean"].isin(codes_trung)].copy()

            # Nhóm 3: Các đối tượng trùng mã (Vừa là KH vừa là NCC) -> Gộp thành 1 bản ghi
# Lấy thông tin nền từ file NCC hiện tại và cập nhật lại Partner_Group
            df_trung = df[df["Partner_Code_Clean"].isin(codes_trung)].copy()
            df_trung["Partner_Group"] = "Khách hàng / Nhà cung cấp"

            # CONCAT CẢ 3 NHÓM: Tạo thành một danh bạ đối tác duy nhất không trùng lặp
            df_final = pd.concat([df_kh_pure, df_ncc_pure, df_trung], ignore_index=True)

            # Loại bỏ cột phụ dùng để map mã sau khi hoàn thành công việc
            df_final = df_final.drop(columns=["Partner_Code_Clean"])

            # Lưu lại danh sách mã trùng vào context đề phòng tầng pg_staging cần dùng cơ chế xóa cũ - chèn mới
            ctx.delete_keys = list(codes_trung)

            return df_final

        return df