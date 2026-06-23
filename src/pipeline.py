"""
pipeline.py - Orchestrator mỏng, gọi các task riêng biệt.

Thay đổi so với phiên bản cũ:
  - run() tách thành upload_task.run() + load_task.run()
  - Thêm run_upload_only() để Airflow có thể gọi Task 1 riêng
  - Thêm run_load_only()  để Airflow có thể gọi Task 2 riêng
  - Thêm rollback() và rollback_batch() để rollback theo ngày
  - create_all_staging_tables() và preview_ddl() giữ nguyên

Trong Airflow bạn sẽ dùng:
  from src.tasks import upload_task, load_task, rollback_task
  và gọi trực tiếp, pipeline.py chỉ dùng cho CLI.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime
from typing import Dict, List, Optional

from src.config_loader import PipelineConfig, load_config
from src.loaders.pg_staging import PgStagingManager
from src.tasks import load_task, rollback_task, upload_task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline")


class ExcelPipeline:
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        self.config: PipelineConfig = load_config(config_path)
        pg_cfg = self.config.connections.postgresql
        self.pg = PgStagingManager(
            config=pg_cfg,
            schema=pg_cfg.get("schema_staging", "bronze"),
        )

    # ------------------------------------------------------------------
    # Bước 1: Upload (có thể gọi độc lập từ Airflow)
    # ------------------------------------------------------------------

    def run_upload(
        self,
        file_path: str,
        file_id: str,
        force_upload: bool = False,
    ) -> upload_task.UploadResult:
        """
        Upload file lên MinIO, ghi FileRegistry.
        Trả về UploadResult để bước 2 dùng.
        """
        batch_id = self._make_batch_id(file_id)
        logger.info(f"Upload | {file_id} | batch={batch_id}")
        return upload_task.run(
            file_path=file_path,
            file_id=file_id,
            config=self.config,
            batch_id=batch_id,
            force_upload=force_upload,
        )

    # ------------------------------------------------------------------
    # Bước 2: Load (có thể gọi độc lập từ Airflow, nhận từ XCom)
    # ------------------------------------------------------------------

    def run_load(
        self,
        file_id: str,
        batch_id: str,
        minio_path: str,
    ) -> load_task.LoadResult:
        """
        Load một file_id từ MinIO vào staging.
        batch_id và minio_path lấy từ UploadResult hoặc XCom.
        """
        logger.info(f"Load | {file_id} | batch={batch_id}")
        result = load_task.run(
            file_id=file_id,
            batch_id=batch_id,
            minio_path=minio_path,
            config=self.config,
        )
        self._print_load_summary(result)
        return result

    # ------------------------------------------------------------------
    # Chạy cả 2 bước liền (CLI convenience, không dùng cho Airflow)
    # ------------------------------------------------------------------

    def run(
        self,
        file_path: str,
        file_id: str,
        dry_run: bool = False,
        force_upload: bool = False,
    ) -> Dict:
        """
        Chạy đủ upload + load cho 1 file.
        Dùng cho CLI. Trong Airflow nên gọi run_upload() và run_load() riêng.
        """
        if dry_run:
            return self._dry_run(file_path, file_id)

        upload_result = self.run_upload(file_path, file_id, force_upload=force_upload)

        if upload_result.skipped:
            logger.info(
                f"[run] {file_id}: File không đổi (MD5 match), "
                f"vẫn tiến hành load lại từ batch cũ: {upload_result.batch_id}"
            )

        load_result = self.run_load(
            file_id=file_id,
            batch_id=upload_result.batch_id,
            minio_path=upload_result.minio_path,
        )

        # Trả về dict tương thích với code cũ
        return {
            sheet: {"status": r.status, "rows": r.rows, "error": r.error}
            for sheet, r in load_result.sheets.items()
        }

    def run_batch(
        self,
        file_pairs: List[tuple],
        dry_run: bool = False,
        force_upload: bool = False,
    ) -> Dict:
        all_results = {}
        for file_path, file_id in file_pairs:
            logger.info(f"\nXử lý: {file_path} [{file_id}]")
            try:
                all_results[file_id] = self.run(
                    file_path, file_id,
                    dry_run=dry_run, force_upload=force_upload,
                )
            except Exception as exc:
                logger.error(f"Pipeline lỗi {file_id}: {exc}")
                all_results[file_id] = {"error": str(exc)}
        return all_results

    # ------------------------------------------------------------------
    # Rollback
    # ------------------------------------------------------------------

    def rollback(
        self,
        file_id: str,
        target_date: date,
    ) -> rollback_task.RollbackResult:
        """Rollback 1 file_id về trạng thái của target_date."""
        logger.info(f"Rollback | {file_id} | target_date={target_date}")
        return rollback_task.run(file_id, target_date, self.config)

    def rollback_batch(
        self,
        file_ids: List[str],
        target_date: date,
    ) -> Dict[str, rollback_task.RollbackResult]:
        """Rollback nhiều file_ids. Nếu file_ids rỗng → rollback tất cả."""
        return rollback_task.run_batch(file_ids, target_date, self.config)

    # ------------------------------------------------------------------
    # Tiện ích quản lý bảng
    # ------------------------------------------------------------------

    def create_all_staging_tables(self):
        logger.info("Tạo tất cả staging tables từ config…")
        for file_cfg in self.config.excel_files:
            logger.info(f"\n  [{file_cfg.file_id}] {file_cfg.description}")
            for sheet_cfg in file_cfg.sheets:
                self.pg.create_or_update_table(sheet_cfg)
                logger.info(f"    ✓ {self.pg.schema}.{sheet_cfg.target_table}")

    def preview_ddl(self, file_id: Optional[str] = None):
        targets = (
            [self.config.get_file_config(file_id)] if file_id else self.config.excel_files
        )
        for file_cfg in targets:
            if not file_cfg:
                continue
            print(f"\n-- ===== {file_cfg.file_id}: {file_cfg.description} =====")
            for sheet_cfg in file_cfg.sheets:
                print(self.pg.generate_ddl_preview(sheet_cfg))
                print()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_batch_id(file_id: str) -> str:
        return f"{file_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    def _dry_run(self, file_path: str, file_id: str) -> Dict:
        from src.config_loader import ExcelFileConfig
        from src.readers.excel_reader import ExcelReader, validate_excel_columns

        file_cfg: ExcelFileConfig = self.config.get_file_config(file_id)
        if not file_cfg:
            raise ValueError(f"Không tìm thấy file_id='{file_id}'")

        logger.info("[DRY RUN] Chỉ validate và preview, không thực thi")

        issues = validate_excel_columns(file_path, file_cfg)
        if issues:
            logger.error(f"Lỗi mapping:\n{issues}")
            return {"dry_run": True, "issues": issues}

        print("\n[DDL PREVIEW]")
        for sheet_cfg in file_cfg.sheets:
            print(self.pg.generate_ddl_preview(sheet_cfg))

        reader = ExcelReader(file_cfg, pg=self.pg)
        previews = {}
        for sheet_cfg, df in reader.read_all_sheets(file_path):
            label = sheet_cfg.sheet_name or f"index_{sheet_cfg.sheet_index}"
            print(f"\n[PREVIEW] {label} → {sheet_cfg.target_table}")
            print(f"  Dòng: {len(df)} | Cột: {list(df.columns)}")
            print(df.head(3).to_string())
            previews[label] = {"rows": len(df), "columns": list(df.columns)}

        return {"dry_run": True, "sheets": previews}

    @staticmethod
    def _print_load_summary(result: load_task.LoadResult):
        logger.info(f"\n{'='*60}")
        logger.info(f"LOAD SUMMARY | {result.file_id} | batch={result.batch_id}")
        logger.info(f"{'='*60}")
        for sheet, r in result.sheets.items():
            if r.status == "success":
                logger.info(f"  ✓ {sheet}: {r.rows:,} dòng")
            else:
                logger.info(f"  ✗ {sheet}: LỖI - {r.error}")
        logger.info(f"  Tổng: {result.total_rows:,} dòng")
        logger.info(f"{'='*60}\n")