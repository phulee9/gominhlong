"""
config_loader.py - Đọc và validate pipeline config từ YAML.

Thay đổi so với phiên bản cũ:
  - SheetConfig thêm `transformer_class` (optional, phiên bản trước đã có)
  - SheetConfig thêm `delete_condition` (optional)
    Dùng cho delete-insert mode và rollback theo kỳ.
    Ví dụ: "Posting_Date >= '2026-06-01' AND Posting_Date <= '2026-06-30'"
"""
from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Dict, Any


@dataclass
class FieldMapping:
    source: str
    target: str
    dtype: str
    nullable: bool = True
    date_format: Optional[str] = None


@dataclass
class MetadataColumn:
    name: str
    dtype: str


@dataclass
class SheetConfig:
    sheet_name: Optional[str]
    sheet_index: Optional[int]
    header_row: int
    data_start_row: int
    target_table: str
    field_mapping: List[FieldMapping]
    metadata_columns: List[MetadataColumn]
    truncate_before_load: bool = True
    upsert_key: Optional[List[str]] = None
    transformer_class: Optional[str] = None
    # Điều kiện WHERE để xóa dữ liệu kỳ cũ trước khi insert.
    # Dùng khi truncate_before_load=False và không muốn upsert.
    # Ví dụ: "Posting_Date >= '2026-06-01' AND Posting_Date <= '2026-06-30'"
    delete_condition: Optional[str] = None

    @property
    def load_mode(self) -> str:
        """
        truncate     → truncate_before_load=True
        delete-insert→ delete_condition có giá trị (ưu tiên sau truncate)
        upsert       → upsert_key có giá trị
        append       → tất cả các trường hợp còn lại
        """
        if self.truncate_before_load:
            return "truncate"
        if self.upsert_key:
            return "upsert"
        return "append"

    @property
    def all_target_columns(self) -> List[str]:
        cols = [m.target for m in self.field_mapping]
        cols += [m.name for m in self.metadata_columns]
        return cols


@dataclass
class ExcelFileConfig:
    file_id: str
    description: str
    type: str
    source_pattern: str
    minio_prefix: str
    sheets: List[SheetConfig]


@dataclass
class ConnectionConfig:
    minio: Dict[str, Any]
    postgresql: Dict[str, Any]


@dataclass
class PipelineConfig:
    connections: ConnectionConfig
    defaults: Dict[str, Any]
    excel_files: List[ExcelFileConfig]

    def get_file_config(self, file_id: str) -> Optional[ExcelFileConfig]:
        return next((f for f in self.excel_files if f.file_id == file_id), None)


def load_config(config_path: str = "config/pipeline_config.yaml") -> PipelineConfig:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file không tồn tại: {config_path}")

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    connections = ConnectionConfig(
        minio=raw["connections"]["minio"],
        postgresql=raw["connections"]["postgresql"],
    )
    defaults = raw.get("defaults", {})

    excel_files = []
    for ef in raw.get("excel_files", []):
        sheets = []
        for s in ef.get("sheets", []):
            field_mappings = [
                FieldMapping(
                    source=fm["source"],
                    target=fm["target"],
                    dtype=fm["dtype"],
                    nullable=fm.get("nullable", True),
                    date_format=fm.get("date_format", defaults.get("date_format")),
                )
                for fm in s.get("field_mapping", [])
            ]
            meta_cols = [
                MetadataColumn(name=mc["name"], dtype=mc["dtype"])
                for mc in s.get("metadata_columns", [])
            ]
            sheets.append(SheetConfig(
                sheet_name=s.get("sheet_name"),
                sheet_index=s.get("sheet_index"),
                header_row=s.get("header_row", 1),
                data_start_row=s.get("data_start_row", 2),
                target_table=s["target_table"],
                field_mapping=field_mappings,
                metadata_columns=meta_cols,
                truncate_before_load=s.get("truncate_before_load", True),
                upsert_key=s.get("upsert_key"),
                transformer_class=s.get("transformer_class"),
                delete_condition=s.get("delete_condition"),      # ← MỚI
            ))
        excel_files.append(ExcelFileConfig(
            file_id=ef["file_id"],
            description=ef.get("description", ""),
            type=ef["type"],
            source_pattern=ef["source_pattern"],
            minio_prefix=ef.get("minio_prefix", ""),
            sheets=sheets,
        ))

    return PipelineConfig(
        connections=connections,
        defaults=defaults,
        excel_files=excel_files,
    )