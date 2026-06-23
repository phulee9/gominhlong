"""
rollback_task.py - Rollback một hoặc nhiều file_id về trạng thái của ngày cũ.

Luồng:
  1. Với mỗi file_id, tìm batch gần nhất có file_date <= target_date
     trong FileRegistry (bỏ qua batch có status='failed')
  2. Xóa dữ liệu hiện tại trong staging table theo batch_id hiện tại
     (dùng delete_condition nếu có, hoặc TRUNCATE nếu là truncate mode)
  3. Gọi load_task.run() với minio_path của batch cũ

Sử dụng:
  # CLI
  python main.py rollback --date 2026-05-30
  python main.py rollback --date 2026-05-30 --ids fact_loan,fact_cashflow

  # Airflow (PythonOperator)
  rollback_task.run_batch(
      file_ids=["fact_loan"],
      target_date=date(2026, 5, 30),
      config=config,
  )
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional

from src.config_loader import PipelineConfig
from src.file_registry import FileRegistry
from src.loaders.pg_staging import PgStagingManager
from src.tasks import load_task

logger = logging.getLogger(__name__)


@dataclass
class RollbackResult:
    file_id: str
    target_date: date
    rolled_back_to_batch: Optional[str] = None
    rolled_back_to_date: Optional[date] = None
    status: str = "pending"     # pending | success | not_found | error
    error: Optional[str] = None
    load_result: Optional[load_task.LoadResult] = None


def run(
    file_id: str,
    target_date: date,
    config: PipelineConfig,
) -> RollbackResult:
    """
    Rollback một file_id về trạng thái của target_date.

    Nếu không tìm được batch phù hợp → trả về status='not_found'.
    """
    result = RollbackResult(file_id=file_id, target_date=target_date)

    pg_cfg = config.connections.postgresql
    registry = FileRegistry(
        config=pg_cfg,
        schema=pg_cfg.get("schema_staging", "bronze"),
    )
    pg = PgStagingManager(
        config=pg_cfg,
        schema=pg_cfg.get("schema_staging", "bronze"),
    )

    # Tìm batch cũ
    old_batch = registry.find_batch_by_date(file_id, target_date)
    if not old_batch:
        logger.warning(
            f"[rollback] {file_id}: Không tìm thấy batch nào có file_date <= {target_date}"
        )
        result.status = "not_found"
        return result

    logger.info(
        f"[rollback] {file_id}: Rollback về batch={old_batch['batch_id']} "
        f"(file_date={old_batch['file_date']})"
    )
    result.rolled_back_to_batch = old_batch["batch_id"]
    result.rolled_back_to_date = old_batch["file_date"]

    file_cfg = config.get_file_config(file_id)
    if not file_cfg:
        result.status = "error"
        result.error = f"Không tìm thấy file_id='{file_id}' trong config"
        return result

    # Xóa dữ liệu hiện tại trước khi load lại
    try:
        for sheet_cfg in file_cfg.sheets:
            table_fqn = f"{pg.schema}.{sheet_cfg.target_table}"
            if hasattr(sheet_cfg, "delete_condition") and sheet_cfg.delete_condition:
                pg.delete_by_condition(table_fqn, sheet_cfg.delete_condition)
                logger.info(f"  Đã xóa {table_fqn} WHERE {sheet_cfg.delete_condition}")
            elif sheet_cfg.truncate_before_load:
                pg.truncate_table(table_fqn)
                logger.info(f"  Đã truncate {table_fqn}")
    except Exception as exc:
        result.status = "error"
        result.error = f"Lỗi khi xóa dữ liệu cũ: {exc}"
        logger.error(f"[rollback] {file_id}: {result.error}")
        return result

    # Re-load từ MinIO batch cũ
    try:
        load_result = load_task.run(
            file_id=file_id,
            batch_id=old_batch["batch_id"],
            minio_path=old_batch["minio_path"],
            config=config,
        )
        result.load_result = load_result
        result.status = "success" if load_result.success else "error"
        if not load_result.success:
            failed = [s for s, r in load_result.sheets.items() if r.status == "error"]
            result.error = f"Sheets lỗi: {failed}"
    except Exception as exc:
        result.status = "error"
        result.error = str(exc)
        logger.error(f"[rollback] {file_id}: {exc}")

    return result


def run_batch(
    file_ids: List[str],
    target_date: date,
    config: PipelineConfig,
) -> Dict[str, RollbackResult]:
    """
    Rollback nhiều file_id cùng lúc.
    Nếu file_ids rỗng → rollback tất cả file_id trong config.
    """
    if not file_ids:
        file_ids = [f.file_id for f in config.excel_files]

    results = {}
    for fid in file_ids:
        logger.info(f"\n{'─'*50}")
        results[fid] = run(fid, target_date, config)

    _print_summary(results)
    return results


def _print_summary(results: Dict[str, RollbackResult]):
    logger.info(f"\n{'='*60}")
    logger.info("ROLLBACK SUMMARY")
    logger.info(f"{'='*60}")
    for fid, r in results.items():
        if r.status == "success":
            rows = r.load_result.total_rows if r.load_result else 0
            logger.info(
                f"  ✓ {fid}: rollback → {r.rolled_back_to_date} "
                f"({rows:,} dòng)"
            )
        elif r.status == "not_found":
            logger.info(f"  – {fid}: không có batch cũ, bỏ qua")
        else:
            logger.info(f"  ✗ {fid}: LỖI - {r.error}")
    logger.info(f"{'='*60}\n")