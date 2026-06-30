from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timedelta

import requests

try:
    from airflow.sdk import dag, task
    from airflow.sdk import Variable
except ImportError:
    from airflow.decorators import dag, task
    from airflow.models import Variable

try:
    from airflow.sdk import Param
except ImportError:
    from airflow.models.param import Param

logger = logging.getLogger(__name__)

WIN_PYTHON_EXE = "/mnt/d/Source/Automation/venv/Scripts/python.exe"
WIN_SCRIPT_PATH = "D:\\Source\\Automation\\report_download.py"
SCRIPT_TIMEOUT_SECONDS = 180 * 60
BOOKMARK_VAR = "misa_last_to_date"
AIRFLOW_API_URL = "http://localhost:8080/api/v1/dags/misa_download_pipeline"
AIRFLOW_USER = "admin"      # ← đổi thành user thật
AIRFLOW_PASS = "Inda1234"      # ← đổi thành pass thật
PLACEHOLDER  = "2026-01-01" # giá trị mặc định UI — bị ignore khi chạy

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def parse_date_param(s: str) -> str:
    s = s.strip()
    if "/" in s:
        return datetime.strptime(s, "%d/%m/%Y").strftime("%d/%m/%Y")
    return datetime.strptime(s, "%Y-%m-%d").strftime("%d/%m/%Y")


@dag(
    dag_id="misa_download_pipeline",
    default_args=default_args,
    description="DAG tải báo cáo MISA | Chưa có thông tin lần chạy gần nhất",
    schedule="0 20 * * 4",
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    params={
        "from_date": Param(
            default=PLACEHOLDER,
            type="string",
            format="date",
            title="Từ ngày",
            description="Giữ nguyên = tự động dùng bookmark. Sửa nếu muốn chạy lại khoảng ngày khác.",
        ),
        "to_date": Param(
            default=PLACEHOLDER,
            type="string",
            format="date",
            title="Đến ngày",
            description="Giữ nguyên = tự động dùng ngày hôm nay. Sửa nếu muốn.",
        ),
    },
    catchup=False,
    tags=["misa", "download"],
)
def misa_download_dag():

    @task
    def run_misa_downloader(**context) -> str:
        params = context.get("params", {})
        today = datetime.now()

        # Đọc bookmark
        try:
            bookmark = Variable.get(BOOKMARK_VAR)
        except KeyError:
            bookmark = None

        auto_from = bookmark or datetime(today.year, 1, 1).strftime("%d/%m/%Y")
        auto_to   = today.strftime("%d/%m/%Y")

        # Đọc params từ UI
        param_from = (params.get("from_date") or "").strip()
        param_to   = (params.get("to_date")   or "").strip()

        # Nếu người dùng không sửa (giữ placeholder) thì dùng auto
        try:
            from_date = (
                parse_date_param(param_from)
                if param_from and param_from != PLACEHOLDER
                else auto_from
            )
        except ValueError:
            from_date = auto_from

        try:
            to_date = (
                parse_date_param(param_to)
                if param_to and param_to != PLACEHOLDER
                else auto_to
            )
        except ValueError:
            to_date = auto_to

        logger.info("=== NGÀY THỰC TẾ SẼ CHẠY ===")
        logger.info("Từ ngày : %s", from_date)
        logger.info("Đến ngày: %s", to_date)
        logger.info("Bookmark: %s", bookmark or "chưa có — dùng đầu năm tài chính")

        cmd = [
            WIN_PYTHON_EXE, "-X", "utf8", WIN_SCRIPT_PATH,
            "--from-date", from_date,
            "--to-date",   to_date,
        ]
        logger.info("Lệnh chạy: %s", " ".join(cmd))

        stderr_lines: list[str] = []

        with subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        ) as proc:
            for line in proc.stdout:
                logger.info(line.rstrip())

            stderr_output = proc.stderr.read()
            if stderr_output:
                for line in stderr_output.splitlines():
                    logger.error("[STDERR] %s", line)
                stderr_lines = stderr_output.splitlines()

            try:
                proc.wait(timeout=SCRIPT_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                proc.kill()
                raise RuntimeError(
                    f"Script vượt quá timeout {SCRIPT_TIMEOUT_SECONDS // 60} phút — đã kill."
                )

            if proc.returncode != 0:
                raise RuntimeError(
                    f"Script thoát với code {proc.returncode}.\n"
                    f"Stderr: {chr(10).join(stderr_lines[-50:])}"
                )

        # Ghi bookmark
        Variable.set(BOOKMARK_VAR, to_date)
        logger.info("Đã cập nhật bookmark → %s", to_date)

        return to_date  # trả to_date cho task tiếp theo

    @task
    def update_dag_description(to_date: str) -> None:
        new_description = (
            f"DAG tải báo cáo MISA tự động | "
            f"Lần chạy gần nhất đến: {to_date} | "
            f"Lần chạy tiếp theo sẽ bắt đầu từ: {to_date}"
        )
        try:
            resp = requests.patch(
                AIRFLOW_API_URL,
                json={"description": new_description},
                auth=(AIRFLOW_USER, AIRFLOW_PASS),
                timeout=10,
            )
            if resp.ok:
                logger.info("Đã cập nhật description DAG → %s", new_description)
            else:
                logger.warning("Cập nhật thất bại: %s %s", resp.status_code, resp.text)
        except Exception as e:
            logger.warning("Không cập nhật được description: %s", e)

    # Kết nối 2 task theo thứ tự
    result = run_misa_downloader()
    update_dag_description(result)


misa_download_dag()