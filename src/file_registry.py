"""
file_registry.py - Theo dõi lịch sử upload/load của từng file_id.

Đây là "nguồn sự thật" cho:
  - upload_task: so sánh MD5 để biết file đã đổi chưa (insert bản ghi mới khi đổi)
  - load_task:   cập nhật trạng thái loaded/failed sau khi load xong
  - rollback_task: tìm batch cũ gần nhất theo file_date để rollback
  - main.py history: xem lại lịch sử upload của 1 file_id

Bảng chính: {schema}._file_registry   (1 dòng = 1 batch = 1 lần upload)
Bảng phụ:   {schema}._file_registry_sheets  (1 dòng = trạng thái load của 1 sheet/table
            trong batch đó — vì 1 file_id có thể có nhiều sheet/target_table)

status của _file_registry:  uploaded -> loaded | failed
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class FileRegistry:
    def __init__(self, config: dict, schema: str = "bronze"):
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
        self._ensure_tables()

    def _connect(self):
        return self.psycopg2.connect(**self.conn_params)

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def _ensure_tables(self):
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}._file_registry (
                        id          BIGSERIAL PRIMARY KEY,
                        file_id     VARCHAR(200) NOT NULL,
                        batch_id    VARCHAR(100) NOT NULL UNIQUE,
                        minio_path  VARCHAR(500) NOT NULL,
                        md5         VARCHAR(64)  NOT NULL,
                        file_date   DATE NOT NULL,
                        status      VARCHAR(50)  NOT NULL DEFAULT 'uploaded',
                        created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
                        updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
                    );
                """)
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS ix_file_registry_file_id_date
                    ON {self.schema}._file_registry (file_id, file_date DESC, created_at DESC);
                """)
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}._file_registry_sheets (
                        id          BIGSERIAL PRIMARY KEY,
                        batch_id    VARCHAR(100) NOT NULL,
                        sheet_name  VARCHAR(200) NOT NULL,
                        status      VARCHAR(50)  NOT NULL,
                        rows_loaded INTEGER,
                        updated_at  TIMESTAMP NOT NULL DEFAULT NOW(),
                        UNIQUE (batch_id, sheet_name)
                    );
                """)
            conn.commit()

    # ------------------------------------------------------------------
    # Ghi nhận upload mới (upload_task)
    # ------------------------------------------------------------------

    def insert(
        self,
        file_id: str,
        batch_id: str,
        minio_path: str,
        md5: str,
        file_date: date,
        status: str = "uploaded",
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}._file_registry
                        (file_id, batch_id, minio_path, md5, file_date, status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (file_id, batch_id, minio_path, md5, file_date, status),
                )
            conn.commit()
        logger.info(f"[file_registry] Ghi nhận batch mới: {file_id} / {batch_id}")

    # ------------------------------------------------------------------
    # Lookup cho upload_task (so sánh MD5)
    # ------------------------------------------------------------------

    def get_latest(self, file_id: str) -> Optional[Dict]:
        """Bản ghi mới nhất (theo created_at) của 1 file_id, bất kể status."""
        with self._connect() as conn:
            with conn.cursor(cursor_factory=self.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT file_id, batch_id, minio_path, md5, file_date, status,
                           created_at, updated_at
                    FROM {self.schema}._file_registry
                    WHERE file_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (file_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    # ------------------------------------------------------------------
    # Lookup cho rollback_task
    # ------------------------------------------------------------------

    def find_batch_by_date(self, file_id: str, target_date: date) -> Optional[Dict]:
        """
        Tìm batch gần nhất có file_date <= target_date, bỏ qua batch 'failed'.
        Ưu tiên file_date lớn nhất, nếu trùng ngày thì lấy created_at mới nhất.
        """
        with self._connect() as conn:
            with conn.cursor(cursor_factory=self.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT file_id, batch_id, minio_path, md5, file_date, status,
                           created_at, updated_at
                    FROM {self.schema}._file_registry
                    WHERE file_id = %s
                      AND file_date <= %s
                      AND status != 'failed'
                    ORDER BY file_date DESC, created_at DESC
                    LIMIT 1
                    """,
                    (file_id, target_date),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    # ------------------------------------------------------------------
    # Cập nhật trạng thái sau load (load_task)
    # ------------------------------------------------------------------

    def update_status(
        self,
        batch_id: str,
        sheet_name: str,
        status: str,
        rows_loaded: Optional[int] = None,
    ) -> None:
        """
        Cập nhật trạng thái load.
        - Ghi/upsert chi tiết theo từng sheet vào _file_registry_sheets
          (để biết chính xác sheet nào loaded/failed trong 1 batch nhiều sheet).
        - Đồng thời cập nhật cột status tổng của _file_registry:
            'failed' nếu BẤT KỲ sheet nào failed, ngược lại 'loaded'.
          (status truyền vào từ load_task đã là status tổng hợp của cả batch,
          nhưng ta tính lại từ bảng con để an toàn khi các sheet update không cùng lúc.)
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}._file_registry_sheets
                        (batch_id, sheet_name, status, rows_loaded, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (batch_id, sheet_name)
                    DO UPDATE SET status = EXCLUDED.status,
                                  rows_loaded = EXCLUDED.rows_loaded,
                                  updated_at = NOW()
                    """,
                    (batch_id, sheet_name, status, rows_loaded),
                )
                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM {self.schema}._file_registry_sheets
                    WHERE batch_id = %s AND status = 'failed'
                    """,
                    (batch_id,),
                )
                any_failed = cur.fetchone()[0] > 0
                overall_status = "failed" if any_failed else "loaded"

                cur.execute(
                    f"""
                    UPDATE {self.schema}._file_registry
                    SET status = %s, updated_at = NOW()
                    WHERE batch_id = %s
                    """,
                    (overall_status, batch_id),
                )
            conn.commit()

    # ------------------------------------------------------------------
    # Lookup cho main.py history
    # ------------------------------------------------------------------

    def list_batches(self, file_id: str, limit: int = 20) -> List[Dict]:
        with self._connect() as conn:
            with conn.cursor(cursor_factory=self.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT batch_id, file_date, status, created_at
                    FROM {self.schema}._file_registry
                    WHERE file_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (file_id, limit),
                )
                return [dict(r) for r in cur.fetchall()]
    # ------------------------------------------------------------------
    # Cleanup bản cũ (upload_task)
    # ------------------------------------------------------------------

    def get_old_versions(self, file_id: str, keep_last: int) -> list[dict]:
        """Trả về các record cũ hơn keep_last bản gần nhất."""
        with self._connect() as conn:
            with conn.cursor(cursor_factory=self.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT batch_id, minio_path
                    FROM {self.schema}._file_registry
                    WHERE file_id = %s
                    ORDER BY created_at DESC
                    OFFSET %s
                    """,
                    (file_id, keep_last),
                )
                return [dict(r) for r in cur.fetchall()]

    def delete_batch(self, batch_id: str) -> None:
        """Xóa 1 batch khỏi registry và sheets liên quan."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self.schema}._file_registry_sheets WHERE batch_id = %s",
                    (batch_id,),
                )
                cur.execute(
                    f"DELETE FROM {self.schema}._file_registry WHERE batch_id = %s",
                    (batch_id,),
                )
            conn.commit()
        logger.info(f"[file_registry] Đã xóa batch: {batch_id}")
