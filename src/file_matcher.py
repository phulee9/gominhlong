"""
file_matcher.py - Map 1 file Excel cụ thể sang (các) file_id liên quan trong config.

Vấn đề cần giải quyết:
  pipeline_config.yaml đã có field `source_pattern` cho mỗi file_id, nhưng
  trước đây không có chỗ nào trong code dùng tới field này. Hệ quả: muốn biết
  "file vừa rơi vào / vừa đổi" thì cần chạy bảng (file_id) nào, người dùng
  phải tự nhớ và gõ --id bằng tay.

  Một file Excel vật lý có thể khớp NHIỀU file_id cùng lúc, ví dụ
  "bc_tin_dung_2026.xlsx" vừa là nguồn của fact_loan, vừa là nguồn của
  fact_credit_limit_summary (2 bảng silver khác nhau lấy dữ liệu từ 1 file).

So khớp dựa trên phần TÊN FILE (basename) của source_pattern, không so khớp
phần thư mục — vì thư mục thật trên máy/Airflow worker có thể khác thư mục
ghi trong config (đường dẫn trong YAML chỉ mang tính mô tả/tài liệu).
"""
from __future__ import annotations

from fnmatch import fnmatch
from pathlib import Path
from typing import List

from src.config_loader import PipelineConfig


def match_file_ids(file_path: str, config: PipelineConfig) -> List[str]:
    """
    Trả về danh sách file_id mà `file_path` khớp source_pattern.
    Có thể trả về 0, 1, hoặc nhiều file_id.
    """
    filename = Path(file_path).name
    matched = []
    for file_cfg in config.excel_files:
        pattern_name = Path(file_cfg.source_pattern).name
        if fnmatch(filename.lower(), pattern_name.lower()):
            matched.append(file_cfg.file_id)
    return matched


def match_files_in_dir(dir_path: str, config: PipelineConfig) -> dict:
    """
    Quét 1 thư mục local, trả về dict {file_path: [file_id, ...]} cho mọi file
    khớp ít nhất 1 file_id trong config.

    Dùng cho:
      - Task "detect_changed_files" đầu DAG: quét landing zone mỗi lần chạy,
        biết file nào vừa xuất hiện/đổi và cần upload+load file_id nào.
      - CLI tiện ích: tự đoán --id khi người dùng chỉ đưa --file.
    """
    result = {}
    for path in Path(dir_path).glob("*"):
        if not path.is_file():
            continue
        ids = match_file_ids(str(path), config)
        if ids:
            result[str(path)] = ids
    return result


def file_ids_for_rollback(file_path: str, config: PipelineConfig) -> List[str]:
    """Alias rõ nghĩa khi dùng cho rollback: 'file này rollback thì kéo theo bảng nào'."""
    return match_file_ids(file_path, config)
