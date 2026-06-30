"""
minio_client.py - Upload/download file Excel từ MinIO (Raw Layer)
"""
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class MinioClient:
    def __init__(self, config: dict):
        try:
            from minio import Minio
            self.client = Minio(
                endpoint=config["endpoint"],
                access_key=config["access_key"],
                secret_key=config["secret_key"],
                secure=config.get("secure", False),
            )
            self.bucket_raw = config["bucket_raw"]
            self._ensure_bucket(self.bucket_raw)
        except ImportError:
            raise ImportError("Cần cài: pip install minio")

    def _ensure_bucket(self, bucket: str):
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)
            logger.info(f"Đã tạo bucket: {bucket}")

    def upload_raw_excel(
        self,
        local_path: str,
        minio_prefix: str,
        file_id: str,
    ) -> str:
        """
        Upload file excel lên MinIO với naming convention:
        {prefix}/{file_id}/{YYYY/MM/DD}/{batch_id}/{filename}
        """
        from datetime import datetime, timezone, timedelta


        local = Path(local_path)
        VN_TZ = timezone(timedelta(hours=7))
        ts = datetime.now(VN_TZ)  # ← giờ VN (UTC+7)
        # Thành
        date_str = ts.strftime("%Y-%m-%d")
        time_str = ts.strftime("%H-%M")
        object_name = f"{minio_prefix.strip('/')}/{date_str}/{time_str}/{local.name}"

        # Tính checksum để verify
        md5 = self._md5_file(local_path)

        self.client.fput_object(
            bucket_name=self.bucket_raw,
            object_name=object_name,
            file_path=local_path,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            metadata={
                "file_id": file_id,
                "original_name": local.name,
                "md5": md5,
                "uploaded_at": ts.isoformat(),
            }
        )
        full_path = f"s3://{self.bucket_raw}/{object_name}"
        logger.info(f"Đã upload: {local.name} -> {full_path}")
        return full_path

    def download_for_processing(self, minio_path: str, local_dest: str) -> str:
        """Download file từ MinIO về local để xử lý"""
        # Strip s3://bucket/ prefix
        bucket, object_name = self._parse_minio_path(minio_path)
        self.client.fget_object(bucket, object_name, local_dest)
        return local_dest

    def _parse_minio_path(self, minio_path: str):
        path = minio_path.replace("s3://", "")
        parts = path.split("/", 1)
        return parts[0], parts[1]

    @staticmethod
    def _md5_file(path: str) -> str:
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    
    def delete_object(self, minio_path: str) -> None:
        """Xóa 1 object trên MinIO. minio_path dạng s3://bucket/prefix/..."""
        bucket, object_name = self._parse_minio_path(minio_path)
        self.client.remove_object(bucket, object_name)
        logger.info(f"[minio] Đã xóa: {minio_path}")
