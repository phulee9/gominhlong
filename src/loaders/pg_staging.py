"""
pg_staging.py - Quản lý bảng staging PostgreSQL (Bronze layer).

Thay đổi so với phiên bản cũ:
  - Thêm delete_by_condition(): dùng cho rollback và delete-insert mode
  - Thêm truncate_table(): public helper để rollback_task gọi
  - load_dataframe() giờ kiểm tra delete_condition trên SheetConfig
    nếu load_mode == "append" (không truncate, không upsert) và
    delete_condition có giá trị → chạy DELETE trước khi INSERT
  - Không còn if/elif theo target_table — logic 100% từ config
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd

from src.config_loader import SheetConfig

logger = logging.getLogger(__name__)

DTYPE_MAP = {
    "varchar": "VARCHAR",
    "text": "TEXT",
    "integer": "INTEGER",
    "int": "INTEGER",
    "int4": "INTEGER",
    "int8": "BIGINT",
    "bigint": "BIGINT",
    "numeric": "NUMERIC",
    "float": "DOUBLE PRECISION",
    "real": "REAL",
    "double precision": "DOUBLE PRECISION",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "boolean": "BOOLEAN",
}


def _pg_type(dtype_str: str) -> str:
    lower = dtype_str.lower()
    for prefix in ("varchar", "numeric", "char"):
        if lower.startswith(prefix) and "(" in lower:
            return dtype_str.upper()
    return DTYPE_MAP.get(lower, dtype_str.upper())


class PgStagingManager:
    def __init__(self, config: dict, schema: str = "staging"):
        try:
            import psycopg2
            import psycopg2.extras
            self.psycopg2 = psycopg2
            self.extras = psycopg2.extras
        except ImportError:
            raise ImportError("Cần cài: pip install psycopg2-binary")

        self.schema = schema
        self.conn_params = {
            "host": config["host"],
            "port": config["port"],
            "dbname": config["database"],
            "user": config["user"],
            "password": config["password"],
        }

    def _connect(self):
        return self.psycopg2.connect(**self.conn_params)

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def create_or_update_table(self, sheet_cfg: SheetConfig) -> str:
        table_fqn = f"{self.schema}.{sheet_cfg.target_table}"
        col_defs = ["    _id BIGSERIAL PRIMARY KEY"]

        for fm in sheet_cfg.field_mapping:
            null_clause = "" if fm.nullable else " NOT NULL"
            col_defs.append(f"    {fm.target} {_pg_type(fm.dtype)}{null_clause}")

        for mc in sheet_cfg.metadata_columns:
            col_defs.append(f"    {mc.name} {_pg_type(mc.dtype)}")

        ddl = (
            f"CREATE TABLE IF NOT EXISTS {table_fqn} (\n"
            + ",\n".join(col_defs)
            + "\n);"
        )

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
                cur.execute(ddl)
                self._sync_columns(cur, sheet_cfg, table_fqn)
            conn.commit()

        logger.info(f"Bảng '{table_fqn}' đã sẵn sàng")
        return ddl

    def _sync_columns(self, cur, sheet_cfg: SheetConfig, table_fqn: str):
        schema, table = table_fqn.split(".")
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        existing_cols = {row[0] for row in cur.fetchall()}

        all_cols = (
            [(fm.target, _pg_type(fm.dtype)) for fm in sheet_cfg.field_mapping]
            + [(mc.name, _pg_type(mc.dtype)) for mc in sheet_cfg.metadata_columns]
        )

        for col_name, col_type in all_cols:
            if col_name not in existing_cols:
                cur.execute(
                    f"ALTER TABLE {table_fqn} ADD COLUMN IF NOT EXISTS {col_name} {col_type};"
                )
                logger.info(f"  Thêm cột mới: {table_fqn}.{col_name} ({col_type})")

    def generate_ddl_preview(self, sheet_cfg: SheetConfig) -> str:
        table_fqn = f"{self.schema}.{sheet_cfg.target_table}"
        col_defs = ["    _id BIGSERIAL PRIMARY KEY"]
        for fm in sheet_cfg.field_mapping:
            null_clause = "" if fm.nullable else " NOT NULL"
            col_defs.append(f"    {fm.target} {_pg_type(fm.dtype)}{null_clause}")
        for mc in sheet_cfg.metadata_columns:
            col_defs.append(f"    {mc.name} {_pg_type(mc.dtype)}")
        return (
            f"-- Staging table: {table_fqn}\n"
            f"CREATE TABLE IF NOT EXISTS {table_fqn} (\n"
            + ",\n".join(col_defs)
            + "\n);"
        )

    # ------------------------------------------------------------------
    # DML: Truncate / Delete
    # ------------------------------------------------------------------

    def truncate_table(self, table_fqn: str, restart_identity: bool = False):
        """Xóa toàn bộ dữ liệu trong bảng. Dùng cho rollback."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                sql = f"TRUNCATE TABLE {table_fqn}"
                if restart_identity:
                    sql += " RESTART IDENTITY"
                sql += ";"
                cur.execute(sql)
            conn.commit()
        logger.info(f"  Đã truncate: {table_fqn}" + (" (reset _id về 1)" if restart_identity else ""))

    def delete_by_condition(self, table_fqn: str, condition: str):
        """
        Xóa dữ liệu theo điều kiện WHERE.
        Dùng cho delete-insert mode và rollback theo kỳ.

        Ví dụ:
            delete_by_condition(
                "bronze.Fact_CashFlow",
                "Posting_Date >= '2026-06-01' AND Posting_Date <= '2026-06-30'"
            )

        CẢNH BÁO: condition được truyền trực tiếp vào SQL.
        Chỉ dùng với giá trị đến từ config YAML — không nhận input từ user.
        """

        if "{" in condition and "}" in condition:
            logger.info(f"  Bỏ qua lệnh xóa thô '{condition}'. Dữ liệu sẽ được tự động xóa chính xác sau khi tải DataFrame.")
            return 0

        sql = f"DELETE FROM {table_fqn} WHERE {condition};"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                deleted = cur.rowcount
            conn.commit()
        logger.info(f"  Đã xóa {deleted:,} dòng từ {table_fqn} WHERE {condition}")
        return deleted

    # ------------------------------------------------------------------
    # DML: Load data
    # ------------------------------------------------------------------

    def load_dataframe(
        self,
        df: pd.DataFrame,
        sheet_cfg: SheetConfig,
        batch_id: str,
        source_file: str,
    ) -> int:
        """
        Load DataFrame vào staging table.

        Load mode (ưu tiên theo thứ tự):
          1. truncate_before_load=True  → TRUNCATE rồi INSERT
          2. delete_condition có giá trị → DELETE WHERE ... rồi INSERT
          3. upsert_key có giá trị       → ON CONFLICT DO UPDATE
          4. Còn lại                     → append (INSERT thuần)
        """
        if df.empty:
            logger.warning(f"DataFrame rỗng, bỏ qua load vào {sheet_cfg.target_table}")
            return 0

        df = df.copy()
        df["_source_file"] = source_file
        df["_loaded_at"] = datetime.utcnow()
        df["_batch_id"] = batch_id

        table_fqn = f"{self.schema}.{sheet_cfg.target_table}"

        # Lấy delete_condition an toàn (field mới, không có trong SheetConfig cũ)
        delete_condition = getattr(sheet_cfg, "delete_condition", None)

        # =====================================================================
        # TỰ ĐỘNG TÌM NGÀY ĐỂ BƠM VÀO LỆNH XÓA
        # =====================================================================
        if delete_condition and "{" in delete_condition and not df.empty:
            # Khai báo các cột ngày tháng chuẩn của hệ thống
            date_columns = ['Posting_Date', 'Snapshot_Date', 'Month', 'Reporting_Date', 'Invoice_Date']
            
            # Quét xem dataframe hiện tại có cột nào khớp không
            target_date_col = next((col for col in date_columns if col in df.columns), None)
            
            if target_date_col:
                # Ép kiểu ngày tháng an toàn và bỏ các dòng trống
                date_series = pd.to_datetime(df[target_date_col], errors='coerce').dropna()
                
                if not date_series.empty:
                    min_date = date_series.min().strftime('%Y-%m-%d')
                    max_date = date_series.max().strftime('%Y-%m-%d')
                    
                    # Bơm ngày thực tế vào các biến {min_date}, {max_date} trong YAML
                    delete_condition = delete_condition.format(
                        min_date=min_date, 
                        max_date=max_date
                    )
                    logger.info(f"  [Auto-Detect] Tự động set điều kiện xóa: {delete_condition}")

        with self._connect() as conn:
            with conn.cursor() as cur:
                is_truncate = (
                    sheet_cfg.load_mode == "truncate"
                    or getattr(sheet_cfg, "truncate_before_load", False)
                )
                logger.info(f"  [DEBUG] sheet={sheet_cfg.sheet_name} truncate_before_load={sheet_cfg.truncate_before_load} load_mode={sheet_cfg.load_mode} is_truncate={is_truncate}")
                if is_truncate:
                    restart = getattr(sheet_cfg, "restart_identity", False)
                    logger.info(f"  restart_identity = {restart}")  # ← thêm dòng này
                    sql = f"TRUNCATE TABLE {table_fqn}"
                    if restart:
                        sql += " RESTART IDENTITY"
                    cur.execute(sql + ";")
                    logger.info(f"  Đã truncate: {table_fqn}" + (" (reset _id → 1)" if restart else ""))
                    count = self._bulk_insert(cur, df, table_fqn)

                elif delete_condition:
                    # Delete-Insert mode: xóa kỳ cũ rồi insert
                    cur.execute(f"DELETE FROM {table_fqn} WHERE {delete_condition};")
                    deleted = cur.rowcount
                    logger.info(
                        f"  Đã xóa {deleted:,} dòng từ {table_fqn} "
                        f"WHERE {delete_condition}"
                    )
                    count = self._bulk_insert(cur, df, table_fqn)

                elif sheet_cfg.load_mode == "upsert" and sheet_cfg.upsert_key:
                    count = self._upsert(cur, df, sheet_cfg, table_fqn)

                else:
                    # Append
                    count = self._bulk_insert(cur, df, table_fqn)

            conn.commit()

        logger.info(f"  Đã load {count:,} dòng → {table_fqn} (mode={sheet_cfg.load_mode})")
        return count

    def _bulk_insert(self, cur, df: pd.DataFrame, table_fqn: str) -> int:
        cols = list(df.columns)
        col_str = ", ".join(cols)
        records = [tuple(row) for row in df.itertuples(index=False)]
        self.extras.execute_values(
            cur,
            f"INSERT INTO {table_fqn} ({col_str}) VALUES %s",
            records,
            page_size=1000,
        )
        return len(records)

    def _upsert(
        self, cur, df: pd.DataFrame, sheet_cfg: SheetConfig, table_fqn: str
    ) -> int:
        cols = list(df.columns)
        col_str = ", ".join(cols)
        conflict_cols = ", ".join(sheet_cfg.upsert_key)
        update_set = ", ".join(
            f"{c} = EXCLUDED.{c}"
            for c in cols
            if c not in sheet_cfg.upsert_key
        )
        records = [tuple(row) for row in df.itertuples(index=False)]
        self.extras.execute_values(
            cur,
            f"""
            INSERT INTO {table_fqn} ({col_str}) VALUES %s
            ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}
            """,
            records,
            page_size=1000,
        )
        return len(records)

    # ------------------------------------------------------------------
    # Lookup (dùng cho transformer cần tra cứu 1 bảng đã load — vd Dim_Bank)
    # ------------------------------------------------------------------

    def fetch_table(self, table_name: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Đọc toàn bộ 1 bảng trong schema hiện tại, trả về DataFrame.
        Dùng cho transformer cần lookup dữ liệu của 1 bảng ĐÃ LOAD trong DB
        (vd: Dim_AccountNumber tra Bank_Code từ Dim_Bank) thay vì đọc 1 file
        Excel sidecar nằm cạnh file đang xử lý — cách cũ gãy khi chạy qua
        MinIO/Airflow vì file lúc đó đã nằm 1 mình trong /tmp.

        CẢNH BÁO: table_name được nối thẳng vào SQL, không qua tham số hoá.
        Chỉ dùng với giá trị cố định từ code (transformer), không nhận trực
        tiếp input từ user.
        """
        table_fqn = f"{self.schema}.{table_name}"
        cols_sql = ", ".join(columns) if columns else "*"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT {cols_sql} FROM {table_fqn}")
                col_names = [d.name for d in cur.description]
                rows = cur.fetchall()
        return pd.DataFrame(rows, columns=col_names)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def get_row_count(self, table_name: str) -> int:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.schema}.{table_name}")
                return cur.fetchone()[0]

    def log_pipeline_run(
        self,
        batch_id: str,
        file_id: str,
        sheet_name: str,
        target_table: str,
        rows_loaded: int,
        status: str,
        error_msg: str = None,
        source_file: str = None,
    ):
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}._pipeline_log (
                        id BIGSERIAL PRIMARY KEY,
                        batch_id VARCHAR(100),
                        file_id VARCHAR(200),
                        sheet_name VARCHAR(200),
                        target_table VARCHAR(200),
                        source_file VARCHAR(500),
                        rows_loaded INTEGER,
                        status VARCHAR(50),
                        error_msg TEXT,
                        started_at TIMESTAMP DEFAULT NOW(),
                        finished_at TIMESTAMP DEFAULT NOW()
                    );
                    INSERT INTO {self.schema}._pipeline_log
                        (batch_id, file_id, sheet_name, target_table,
                         source_file, rows_loaded, status, error_msg)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (batch_id, file_id, sheet_name, target_table,
                     source_file, rows_loaded, status, error_msg),
                )
            conn.commit()