"""
main.py - CLI entrypoint cho Excel Pipeline

Các subcommand:
  run          Chạy đủ upload + load cho 1 file (tiện lợi, không dùng cho Airflow)
  upload       Chỉ upload lên MinIO + ghi FileRegistry (Task 1)
  load         Chỉ load từ MinIO vào DB (Task 2, cần batch_id + minio_path)
  rollback     Rollback 1 hoặc nhiều file_id về ngày cũ
  run-batch    Chạy nhiều files cùng lúc
  detect       Quét 1 thư mục, in ra file nào khớp file_id nào (dùng source_pattern)
  setup-tables Tạo tất cả staging tables từ config
  preview-ddl  In DDL preview
  validate     Validate cột Excel với config
  history      Xem lịch sử upload của 1 file_id

LƯU Ý: `--id` giờ là OPTIONAL cho run/upload/validate. Nếu không truyền, CLI tự
suy ra (các) file_id khớp với tên file qua `source_pattern` trong config
(xem src/file_matcher.py). Nếu 1 file khớp NHIỀU file_id (vd file báo cáo tín
dụng sinh ra cả fact_loan + fact_credit_limit_summary), lệnh sẽ chạy lần lượt
TẤT CẢ file_id khớp được.
"""
import argparse
import sys
from datetime import date
from pathlib import Path



def main():
    parser = argparse.ArgumentParser(
        description="Excel → MinIO → PostgreSQL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ:
  # Chạy đủ upload + load
  python main.py run --file bao_cao.xlsx --id fact_cashflow

  # Tách 2 bước (Airflow style)
  python main.py upload --file bao_cao.xlsx --id fact_cashflow
  python main.py load --id fact_cashflow --batch-id fact_cashflow_20260601_123456_abc --minio-path s3://raw-excel/...

  # Rollback 1 file về tuần trước
  python main.py rollback --date 2026-05-30 --ids fact_loan

  # Rollback toàn bộ về ngày cũ
  python main.py rollback --date 2026-05-30

  # Xem lịch sử upload
  python main.py history --id fact_cashflow
        """,
    )
    parser.add_argument(
        "--config", default="config/pipeline_config.yaml",
        help="Đường dẫn tới pipeline_config.yaml"
    )

    sub = parser.add_subparsers(dest="command")

    # --- run (đủ 2 bước) ---
    p_run = sub.add_parser("run", help="Upload + load cho 1 file")
    p_run.add_argument("--file", required=True)
    p_run.add_argument("--id", dest="file_id", default=None,
                       help="file_id. Bỏ qua = tự suy ra từ source_pattern (có thể ra nhiều file_id)")
    p_run.add_argument("--dry-run", action="store_true")
    p_run.add_argument("--force-upload", action="store_true",
                       help="Bỏ qua MD5 check, luôn upload")

    # --- upload (Task 1 riêng) ---
    p_up = sub.add_parser("upload", help="Chỉ upload MinIO + FileRegistry")
    p_up.add_argument("--file", required=True)
    p_up.add_argument("--id", dest="file_id", default=None,
                      help="file_id. Bỏ qua = tự suy ra từ source_pattern")
    p_up.add_argument("--force-upload", action="store_true")

    # --- load (Task 2 riêng) ---
    p_load = sub.add_parser("load", help="Chỉ load từ MinIO vào DB")
    p_load.add_argument("--id", required=True, dest="file_id")
    p_load.add_argument("--batch-id", required=True)
    p_load.add_argument("--minio-path", required=True)

    # --- rollback ---
    p_rb = sub.add_parser("rollback", help="Rollback về trạng thái ngày cũ")
    p_rb.add_argument(
        "--date", required=True,
        help="Ngày rollback, định dạng YYYY-MM-DD",
        metavar="YYYY-MM-DD",
    )
    p_rb.add_argument(
        "--ids", default="",
        help="Danh sách file_id cách nhau bởi dấu phẩy. Bỏ qua = rollback tất cả.",
    )

    # --- run-batch ---
    p_batch = sub.add_parser("run-batch", help="Chạy nhiều files")
    p_batch.add_argument("--files", required=True,
                         help="Danh sách 'file:id,file:id,...'")
    p_batch.add_argument("--dry-run", action="store_true")
    p_batch.add_argument("--force-upload", action="store_true")

    # --- detect ---
    p_detect = sub.add_parser(
        "detect", help="Quét 1 thư mục, in ra file nào khớp file_id nào (source_pattern)"
    )
    p_detect.add_argument("--dir", required=True, help="Thư mục cần quét")

    # --- setup-tables ---
    sub.add_parser("setup-tables", help="Tạo tất cả staging tables từ config")

    # --- preview-ddl ---
    p_ddl = sub.add_parser("preview-ddl", help="In DDL preview")
    p_ddl.add_argument("--id", dest="file_id", help="file_id cụ thể (bỏ qua = tất cả)")

    # --- validate ---
    p_val = sub.add_parser("validate", help="Validate cột Excel với config")
    p_val.add_argument("--file", required=True)
    p_val.add_argument("--id", dest="file_id", default=None,
                       help="file_id. Bỏ qua = tự suy ra từ source_pattern")

    # --- history ---
    p_hist = sub.add_parser("history", help="Xem lịch sử upload của 1 file_id")
    p_hist.add_argument("--id", required=True, dest="file_id")
    p_hist.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    sys.path.insert(0, str(Path(__file__).parent))
    from src.pipeline import ExcelPipeline
    from src.config_loader import load_config
    from src.file_matcher import match_file_ids

    def resolve_file_ids(file_path: str, explicit_id) -> list:
        """--id được truyền thì dùng đúng nó; không thì tự suy ra từ source_pattern."""
        if explicit_id:
            return [explicit_id]
        cfg = load_config(args.config)
        matched = match_file_ids(file_path, cfg)
        if not matched:
            print(
                f"❌ Không khớp được file_id nào cho '{file_path}' qua source_pattern.\n"
                f"   Dùng --id để chỉ định trực tiếp, hoặc kiểm tra lại "
                f"source_pattern trong {args.config}."
            )
            sys.exit(1)
        if len(matched) > 1:
            print(f"ℹ️  '{Path(file_path).name}' khớp {len(matched)} file_id: {matched}")
        return matched

    pipeline = ExcelPipeline(config_path=args.config)

    # ------------------------------------------------------------------
    if args.command == "run":
        file_ids = resolve_file_ids(args.file, args.file_id)
        has_error = False
        for fid in file_ids:
            results = pipeline.run(
                file_path=args.file,
                file_id=fid,
                dry_run=args.dry_run,
                force_upload=args.force_upload,
            )
            if any(r.get("status") == "error" for r in results.values() if isinstance(r, dict)):
                has_error = True
        sys.exit(1 if has_error else 0)

    elif args.command == "upload":
        file_ids = resolve_file_ids(args.file, args.file_id)
        for fid in file_ids:
            result = pipeline.run_upload(
                file_path=args.file,
                file_id=fid,
                force_upload=args.force_upload,
            )
            if result.skipped:
                print(f"⏭  [{fid}] Skipped (MD5 không đổi). batch_id={result.batch_id}")
            else:
                print(f"✅ [{fid}] Uploaded. batch_id={result.batch_id}")
                print(f"   minio_path={result.minio_path}")
            # In ra để dùng trong shell script hoặc xem XCom
            print(f"\n# Dùng để chạy load tiếp:")
            print(
                f"python main.py load --id {fid} "
                f"--batch-id {result.batch_id} "
                f"--minio-path {result.minio_path}\n"
            )

    elif args.command == "load":
        result = pipeline.run_load(
            file_id=args.file_id,
            batch_id=args.batch_id,
            minio_path=args.minio_path,
        )
        sys.exit(0 if result.success else 1)

    elif args.command == "rollback":
        try:
            target_date = date.fromisoformat(args.date)
        except ValueError:
            print(f"❌ Định dạng ngày không hợp lệ: {args.date}. Dùng YYYY-MM-DD.")
            sys.exit(1)

        file_ids = [x.strip() for x in args.ids.split(",") if x.strip()]
        results = pipeline.rollback_batch(file_ids, target_date)

        has_error = any(r.status == "error" for r in results.values())
        sys.exit(1 if has_error else 0)

    elif args.command == "run-batch":
        pairs = []
        for item in args.files.split(","):
            fp, fid = item.strip().split(":")
            pairs.append((fp.strip(), fid.strip()))
        pipeline.run_batch(pairs, dry_run=args.dry_run, force_upload=args.force_upload)

    elif args.command == "detect":
        from src.file_matcher import match_files_in_dir
        cfg = load_config(args.config)
        matches = match_files_in_dir(args.dir, cfg)
        if not matches:
            print(f"Không có file nào trong '{args.dir}' khớp source_pattern nào trong config.")
        else:
            print(f"Tìm thấy {len(matches)} file khớp:\n")
            for file_path, file_ids in matches.items():
                print(f"  {file_path}")
                for fid in file_ids:
                    print(f"    -> {fid}")

    elif args.command == "setup-tables":
        pipeline.create_all_staging_tables()

    elif args.command == "preview-ddl":
        pipeline.preview_ddl(file_id=getattr(args, "file_id", None))

    elif args.command == "validate":
        from src.readers.excel_reader import validate_excel_columns
        file_ids = resolve_file_ids(args.file, args.file_id)
        cfg = load_config(args.config)
        any_issue = False
        for fid in file_ids:
            file_cfg = cfg.get_file_config(fid)
            if not file_cfg:
                print(f"❌ Không tìm thấy file_id: {fid}")
                any_issue = True
                continue
            issues = validate_excel_columns(args.file, file_cfg)
            if issues:
                any_issue = True
                print(f"❌ [{fid}] Lỗi mapping cột:")
                for sheet, cols in issues.items():
                    print(f"  Sheet '{sheet}': {cols}")
            else:
                print(f"✅ [{fid}] Tất cả cột đều hợp lệ!")
        sys.exit(1 if any_issue else 0)

    elif args.command == "history":
        from src.config_loader import load_config
        from src.file_registry import FileRegistry
        cfg = load_config(args.config)
        pg_cfg = cfg.connections.postgresql
        registry = FileRegistry(
            config=pg_cfg,
            schema=pg_cfg.get("schema_staging", "bronze"),
        )
        batches = registry.list_batches(args.file_id, limit=args.limit)
        if not batches:
            print(f"Chưa có lịch sử upload cho '{args.file_id}'")
        else:
            print(f"\nLịch sử upload: {args.file_id} (mới nhất trước)")
            print(f"{'batch_id':<45} {'file_date':<12} {'status':<12} {'created_at'}")
            print("─" * 100)
            for b in batches:
                print(
                    f"{b['batch_id']:<45} "
                    f"{str(b['file_date']):<12} "
                    f"{b['status']:<12} "
                    f"{b['created_at']}"
                )


if __name__ == "__main__":
    main()