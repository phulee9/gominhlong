"""
excel_reader.py - Đọc Excel theo config, hỗ trợ single_table và multi_table

ĐÃ REFACTOR: Trước đây file này có ~650 dòng if/elif (1 nhánh / 1 target_table)
nhúng cứng toàn bộ business logic của 18 bảng silver ngay trong
_read_one_sheet(). Giờ ExcelReader chỉ còn lo phần GENERIC (đọc Excel, chọn
cột, rename, cast dtype). Business logic riêng từng bảng đã chuyển sang
src/transformers/<table>.py, khai báo qua field `transformer_class` trong
pipeline_config.yaml. Xem src/transformers/base.py để biết interface.

Lợi ích:
  - Sửa logic 1 bảng không còn đụng tới code của 17 bảng khác
  - Mỗi transformer test riêng được (input/output là DataFrame, không phụ
    thuộc DB/MinIO)
  - Nhìn vào config biết ngay bảng nào có logic đặc biệt, bảng nào không
"""
import logging
from typing import Dict, Iterator, Optional, Tuple

import pandas as pd

from src.config_loader import ExcelFileConfig, SheetConfig
from src.transformers import TransformContext, get_transformer

logger = logging.getLogger(__name__)


class ExcelReader:
    """
    Đọc file Excel theo SheetConfig, trả về DataFrame đã được:
    - Chọn đúng header row
    - Lọc đúng các cột cần thiết (field_mapping)
    - Rename cột sang tên target
    - Áp transformer riêng của bảng (nếu sheet_cfg.transformer_class có khai báo)
    - Cast kiểu dữ liệu cơ bản
    """

    def __init__(self, file_config: ExcelFileConfig, null_values: list = None, pg=None):
        """
        Args:
            file_config: ExcelFileConfig đã parse từ YAML
            null_values: danh sách giá trị coi là NULL khi đọc Excel
            pg: PgStagingManager (optional). Truyền vào khi transformer cần
                lookup dữ liệu từ 1 bảng ĐÃ LOAD trong DB (vd: Dim_AccountNumber
                cần tra Dim_Bank). Không truyền (None) vẫn đọc/transform được
                bình thường — transformer nào cần DB sẽ tự log cảnh báo và bỏ
                qua phần lookup (vd: lúc preview/dry-run).
        """
        self.file_config = file_config
        self.null_values = null_values or ["", "N/A", "NULL", "null", "None", "-"]
        self.pg = pg

    def read_all_sheets(
        self, file_path: str
    ) -> Iterator[Tuple[SheetConfig, pd.DataFrame]]:
        """
        Đọc tất cả sheets được khai báo trong config.
        Yield (sheet_config, dataframe) cho từng sheet.
        """
        for sheet_cfg in self.file_config.sheets:
            logger.info(
                f"[{self.file_config.file_id}] Đọc sheet: "
                f"{sheet_cfg.sheet_name or f'index={sheet_cfg.sheet_index}'}"
            )
            df = self._read_one_sheet(file_path, sheet_cfg)
            logger.info(
                f"  -> {len(df)} dòng, {len(df.columns)} cột sau khi mapping"
            )
            yield sheet_cfg, df

    def _read_one_sheet(self, file_path: str, sheet_cfg: SheetConfig) -> pd.DataFrame:
        # Xác định sheet
        sheet = sheet_cfg.sheet_name if sheet_cfg.sheet_name else sheet_cfg.sheet_index

        # Xử lý header cho cả trường hợp là list (gộp dòng) hoặc int (1 dòng)
        if isinstance(sheet_cfg.header_row, list):
            header_row_0 = [h - 1 for h in sheet_cfg.header_row]
        else:
            header_row_0 = sheet_cfg.header_row - 1

        try:
            # Đọc file bằng header_row_0 trực tiếp (Pandas sẽ tự lo việc bỏ qua các dòng rác phía trên)
            df_raw = pd.read_excel(
                file_path,
                sheet_name=sheet,
                header=header_row_0,
                na_values=self.null_values,
                dtype=str,              # Đọc tất cả dưới dạng string, cast sau
                engine="openpyxl",
            )

        except Exception as e:
            raise RuntimeError(
                f"Lỗi đọc sheet '{sheet}' trong file '{file_path}': {e}"
            ) from e

        # --- FLATTEN MULTI-INDEX TRƯỚC ---
        if isinstance(df_raw.columns, pd.MultiIndex):
            new_cols = []
            for col in df_raw.columns:
                top = str(col[0]).strip()
                bottom = str(col[1]).strip()
                if "Unnamed" in bottom or bottom == "" or bottom.lower() == "nan":
                    new_cols.append(top)
                else:
                    new_cols.append(f"{top}_{bottom}")
            df_raw.columns = new_cols
        # ---------------------------------------------------------

        # Strip whitespace khỏi tên cột
        df_raw.columns = [str(c).strip() for c in df_raw.columns]
        # Chỉ lấy các cột có trong field_mapping
        df = self._apply_field_mapping(df_raw, sheet_cfg)

        # --- BUSINESS LOGIC RIÊNG TỪNG BẢNG (nếu có khai báo transformer_class) ---
        if sheet_cfg.transformer_class:
            transformer = get_transformer(sheet_cfg.transformer_class)
            ctx = TransformContext(
                file_path=file_path,
                file_id=self.file_config.file_id,
                sheet=sheet,
                sheet_cfg=sheet_cfg,
                file_config=self.file_config,
                df_raw=df_raw,
                lookup=self._lookup,
            )
            df = transformer.transform(df, ctx)
        # ---------------------------------------------------------------------

        # Cast kiểu dữ liệu
        df = self._cast_dtypes(df, sheet_cfg)

        # Xóa dòng hoàn toàn trống
        df = df.dropna(how="all")

        # --- BẮT ĐẦU FIX LỖI NaT ---
        # Chuyển đổi tất cả NaN, NaT của Pandas thành None để PostgreSQL hiểu là NULL
        df = df.astype(object).where(pd.notnull(df), None)
        # ---------------------------

        return df

    def _lookup(self, table_name: str) -> Optional[pd.DataFrame]:
        """
        Cho transformer tra cứu toàn bộ 1 bảng ĐÃ LOAD trong DB (vd: "Dim_Bank").
        Trả về DataFrame rỗng nếu không có kết nối DB (vd: dry-run/preview) hoặc
        bảng chưa có dữ liệu — transformer tự quyết định xử lý thế nào trong
        trường hợp này (thường là: log cảnh báo, để cột lookup = None).
        """
        if self.pg is None:
            logger.warning(
                f"[ExcelReader] Không có kết nối DB (pg=None) để lookup '{table_name}'."
            )
            return pd.DataFrame()
        try:
            return self.pg.fetch_table(table_name)
        except Exception as e:
            logger.warning(f"[ExcelReader] Lỗi khi lookup bảng '{table_name}': {e}")
            return pd.DataFrame()

    def _apply_field_mapping(
        self, df_raw: pd.DataFrame, sheet_cfg: SheetConfig
    ) -> pd.DataFrame:
        """Chọn và rename cột theo field_mapping config"""
        source_cols = [fm.source.strip() for fm in sheet_cfg.field_mapping]
        target_cols = {fm.source.strip(): fm.target for fm in sheet_cfg.field_mapping}

        # Kiểm tra cột thiếu
        missing = [c for c in source_cols if c not in df_raw.columns]
        if missing:
            available = list(df_raw.columns)
            raise ValueError(
                f"Các cột sau không tìm thấy trong sheet "
                f"'{sheet_cfg.sheet_name}':\n"
                f"  Thiếu  : {missing}\n"
                f"  Có sẵn : {available}"
            )

        df = df_raw[source_cols].copy()
        df = df.rename(columns=target_cols)
        return df

    def _cast_dtypes(self, df: pd.DataFrame, sheet_cfg: SheetConfig) -> pd.DataFrame:
        """Cast kiểu dữ liệu dựa trên dtype trong config"""
        for fm in sheet_cfg.field_mapping:
            col = fm.target
            if col not in df.columns:
                continue
            dtype_lower = fm.dtype.lower()

            try:
                if dtype_lower.startswith("varchar") or dtype_lower == "text":
                    df[col] = df[col].astype(str).where(df[col].notna(), None)

                elif dtype_lower in ("integer", "int", "int4", "int8", "bigint"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

                elif dtype_lower.startswith("numeric") or dtype_lower in ("float", "real", "double precision"):
                    df[col] = pd.to_numeric(df[col], errors="coerce")

                elif dtype_lower == "date":
                    # Để Pandas tự động nhận diện mọi định dạng ngày tháng (Tây hay Ta đều hiểu)
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

                elif dtype_lower.startswith("timestamp"):
                    df[col] = pd.to_datetime(df[col], errors="coerce")

                elif dtype_lower == "boolean":
                    df[col] = df[col].map(
                        {"true": True, "True": True, "1": True,
                         "false": False, "False": False, "0": False}
                    )

            except Exception as e:
                logger.warning(f"  Không thể cast cột '{col}' sang '{fm.dtype}': {e}")

        return df


def validate_excel_columns(file_path: str, file_config: ExcelFileConfig) -> Dict[str, list]:
    """
    Tiện ích kiểm tra cột trước khi chạy pipeline.
    Trả về dict {sheet_name: [missing_columns]}
    """
    issues = {}
    xl = pd.ExcelFile(file_path, engine="openpyxl")

    for sheet_cfg in file_config.sheets:
        sheet = sheet_cfg.sheet_name or sheet_cfg.sheet_index
        if sheet not in xl.sheet_names and not isinstance(sheet, int):
            issues[str(sheet)] = [f"Sheet '{sheet}' không tồn tại. Có: {xl.sheet_names}"]
            continue

        if isinstance(sheet_cfg.header_row, list):
            # Nếu là list [8, 9] thì trừ 1 cho từng phần tử -> [7, 8]
            header_row_0 = [h - 1 for h in sheet_cfg.header_row]
        else:
            # Nếu chỉ là số nguyên bình thường thì trừ 1 như cũ
            header_row_0 = sheet_cfg.header_row - 1

        df_head = pd.read_excel(
            file_path, sheet_name=sheet, header=header_row_0,
            nrows=1, engine="openpyxl"
        )

        # --- XỬ LÝ LÀM PHẲNG CỘT MULTI-INDEX ---
        if isinstance(df_head.columns, pd.MultiIndex):
            new_cols = []
            for col in df_head.columns:
                top = str(col[0]).strip()
                bottom = str(col[1]).strip()
                if "Unnamed" in bottom or bottom == "" or bottom.lower() == "nan":
                    new_cols.append(top)
                else:
                    new_cols.append(f"{top}_{bottom}")
            df_head.columns = new_cols
        # ---------------------------------------

        actual_cols = [str(c).strip() for c in df_head.columns]
        logger.debug(f"Sheet {sheet} detected columns: {actual_cols}")

        # Trong validate_excel_columns, thêm strip() vào expected_cols
        expected_cols = [fm.source.strip() for fm in sheet_cfg.field_mapping]
        missing = [c for c in expected_cols if c not in actual_cols]

        if missing:
            issues[str(sheet)] = missing

    return issues
