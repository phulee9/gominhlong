"""
Khách hàng double-click shortcut "Đăng nhập MISA" trên desktop.
PHƯƠNG ÁN B (session cookie) — bản hoàn chỉnh có LOGGING:
  MISA phát SESSION COOKIE do server ép — chết theo browser theo thiết
  kế, profile persistent một mình không giữ được đăng nhập. Giải pháp:
  xuất cookie ra misa_cookies.json lúc còn sống trong RAM; downloader
  bơm lại mỗi lần chạy và tự xuất bản mới → session tự gia hạn.
  - Export cookie mỗi 4s trong lúc chờ (chống khách bấm X sớm).
  - Backup/restore: đăng nhập hỏng không đè hỏng file cookie tốt đang có.
  - Kiểm chứng có bơm cookie — test đúng cơ chế downloader dùng thật.
  - _page_login_state: marker khẳng định xét TRƯỚC, ô mật khẩu check
    bằng is_visible() (count() dính cả ô ẩn còn sót trong DOM SPA →
    từng gây bug "thấy 2 công ty rồi mà không tự đóng").
  - TỰ ĐỘNG ĐĂNG NHẬP LẠI (không cần khách gõ gì trong đa số trường hợp):
    dựa trên cơ chế "thiết bị tin cậy" của MISA — profile này đã từng
    đăng nhập đầy đủ ít nhất 1 lần, thông tin tin cậy lưu trong
    localStorage của chính profile (sống lâu hơn session cookie hay chết
    theo browser theo thiết kế). Khi session hết hạn, script tự:
      1. Điền định danh đăng nhập (LOGIN_IDENTIFIER) + bấm "Đăng nhập".
      2. Tự bấm "Bỏ qua, tiếp tục làm việc" khi màn hình xác thực hiện ra
         (selector lấy từ Playwright codegen thật).
    Chỉ khi gặp Ô MẬT KHẨU THẬT hiển thị (thiết bị mất trạng thái tin
    cậy — hiếm) mới cần khách tự gõ tay (không tự động hoá việc lưu mật
    khẩu/OTP). Số lần bypass còn lại (VD "10/10") do MISA tự quản lý phía
    server — khách tự theo dõi bên MISA; khi hết quota, 1 lần đăng nhập
    tay đầy đủ sẽ tự reset lại.
  - LOG: in màn hình + ghi dang_nhap_misa.log (traceback đầy đủ khi
    lỗi) — chạy pythonw không có console vẫn đọc lại được.
  BẢO MẬT: misa_cookies.json tương đương mật khẩu — không copy ra ngoài.
CHẾ ĐỘ TỰ SOI:  python dang_nhap_misa.py --kiem-tra
"""
import asyncio
import ctypes
import json
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright

PROFILE_DIR = Path(r"D:\Source\Automation\misa_automation_profile")
COOKIES_FILE = Path(r"D:\Source\Automation\misa_cookies.json")
MISA_LOGIN_URL = "https://aspapp.misa.vn/App/Account/Join"
VERIFY_SCREENSHOT = Path(r"D:\Source\Automation\kiem_chung_misa.png")
LOG_FILE = Path(r"D:\Source\Automation\dang_nhap_misa.log")
LOGIN_WAIT_MINUTES = 15
VERIFY_WAIT_SECONDS = 60

# Nút "Bỏ qua xác thực" — selector lấy từ Playwright codegen thật (không
# đoán bừa, đúng chuẩn các selector khác trong file này).
SKIP_VERIFICATION_BUTTON_NAME = "Bỏ qua, tiếp tục làm việc"

# Định danh đăng nhập (số điện thoại/email) — THÔNG TIN NHẠY CẢM tương tự
# misa_cookies.json, không chia sẻ/copy ra ngoài. Lấy nguyên văn từ
# Playwright codegen thật. Placeholder ô nhập giữ đúng chuỗi codegen ghi
# nhận (kể cả khoảng trắng cuối) để get_by_placeholder khớp chắc chắn.
LOGIN_IDENTIFIER = "0386477302"
LOGIN_IDENTIFIER_PLACEHOLDER = "Email hoặc số điện thoại "

SCHEDULE_NOTE = "Báo cáo sẽ tự động tải vào 20:00 thứ Năm hàng tuần."
# Tên gợi ý nhận diện màn hình chọn công ty — khớp 1 phần là đủ.
COMPANY_TEXT_HINTS = ["KIỂM TOÁN DTH", "TẬP ĐOÀN GỖ", "Truy cập", "Xem dữ liệu"]
ICON_INFO, ICON_WARN, ICON_ERROR = 0x40, 0x30, 0x10


def log(msg: str) -> None:
    """In ra console VÀ ghi vào file log (pythonw không console vẫn đọc
    lại được). Không bao giờ để lỗi ghi log làm chết luồng chính."""
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    try:
        print(line, flush=True)
    except Exception:
        pass
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def log_exc(prefix: str) -> None:
    log(f"{prefix}: {traceback.format_exc().strip()}")


def popup(title: str, message: str, icon: int = ICON_INFO) -> None:
    log(f"[popup] {title}")
    ctypes.windll.user32.MessageBoxW(0, message, title, icon)


async def inject_saved_cookies(context) -> bool:
    """Bơm cookie đã lưu vào context. True nếu bơm được ít nhất 1 cookie."""
    try:
        cookies = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
        if not cookies:
            log("[cookie] file JSON rỗng — không có gì để bơm.")
            return False
        await context.add_cookies(cookies)
        log(f"[cookie] đã bơm {len(cookies)} cookie từ {COOKIES_FILE.name}.")
        return True
    except FileNotFoundError:
        log(f"[cookie] chưa có {COOKIES_FILE.name} — bỏ qua bước bơm.")
        return False
    except Exception:
        log_exc("[cookie] bơm cookie LỖI")
        return False


async def export_cookies(context) -> int:
    """Xuất toàn bộ cookie hiện tại ra JSON. Trả về số cookie đã lưu."""
    cookies = await context.cookies()
    COOKIES_FILE.write_text(
        json.dumps(cookies, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    return len(cookies)


async def try_skip_verification(context) -> int:
    """Quét MỌI tab đang mở trong context, bấm nút 'Bỏ qua, tiếp tục làm
    việc' nếu đang hiển thị (màn hình xác thực thiết bị MISA chèn sau khi
    khách bấm "Đăng nhập"). Bấm trên mọi tab (không chỉ tab đầu) vì luồng
    đăng nhập MISA có thể mở thêm tab/popup.

    Trả về số lần vừa bấm được trong lượt gọi này (0 nếu không thấy nút
    trên tab nào — hoàn toàn bình thường, không phải lỗi).
    """
    clicked = 0
    for pg in list(context.pages):
        try:
            btn = pg.get_by_role("button", name=SKIP_VERIFICATION_BUTTON_NAME)
            if await btn.first.is_visible():
                await btn.first.click(timeout=3000)
                clicked += 1
        except Exception:
            # Không thấy nút trên tab này, hoặc tab vừa đóng giữa chừng —
            # bỏ qua, thử tab tiếp theo.
            continue
    return clicked


async def try_auto_login(page) -> str:
    """Tự điền định danh đăng nhập + bấm 'Đăng nhập' — KHÔNG cần mật khẩu
    hay OTP — dựa trên cơ chế "thiết bị tin cậy" của MISA: profile này
    (PROFILE_DIR) đã từng đăng nhập đầy đủ ít nhất 1 lần, thông tin tin
    cậy lưu trong localStorage của chính profile (sống lâu hơn session
    cookie hay chết theo browser). Các bước lấy NGUYÊN VĂN từ Playwright
    codegen thật:
        page.get_by_placeholder("Email hoặc số điện thoại ").fill(...)
        page.get_by_role("button", name="Đăng nhập").click()
        (sau đó màn hình "Bỏ qua, tiếp tục làm việc" hiện ra — xử lý ở
        try_skip_verification, gọi riêng ở vòng lặp ngoài)

    Trả về (để vòng lặp gọi hàm quyết định bước tiếp theo):
      'clicked_login'  — vừa điền + bấm "Đăng nhập" xong, cần đợi vòng
                          lặp sau xem ra màn hình gì tiếp theo.
      'need_password'  — gặp Ô MẬT KHẨU THẬT đang hiển thị — vượt quá khả
                          năng tự động (không lưu mật khẩu), phải dừng lại
                          chờ khách tự gõ (main() vẫn giữ luồng chờ +
                          popup cũ làm phương án dự phòng).
      'no_action'       — chưa có gì để làm ở bước này (trang chưa load
                          xong, hoặc đã ở màn hình khác) — thử lại ở vòng
                          lặp sau.
    """
    try:
        pw_field = page.locator("input[type='password']")
        if await pw_field.first.is_visible():
            return "need_password"
    except Exception:
        pass

    try:
        id_field = page.get_by_placeholder(LOGIN_IDENTIFIER_PLACEHOLDER)
        if await id_field.first.is_visible():
            current = (await id_field.first.input_value()).strip()
            if not current:
                await id_field.first.fill(LOGIN_IDENTIFIER)
            await page.get_by_role("button", name="Đăng nhập").click(timeout=5000)
            return "clicked_login"
    except Exception:
        pass

    return "no_action"


async def _page_login_state(page) -> str | None:
    """'in' = chắc chắn ĐÃ đăng nhập; 'out' = chắc chắn CHƯA (ô mật khẩu
    ĐANG HIỂN THỊ); None = chưa kết luận được.
    Marker khẳng định xét TRƯỚC; ô mật khẩu check bằng is_visible() —
    count() dính cả ô ẩn còn sót trong DOM sau khi SPA chuyển màn."""
    try:
        if "actasp.misa.vn" in page.url:
            return "in"
        probes = [
            page.locator(".invite-company-ele").first,
            page.locator("button.access").first,
            page.locator(".name-company").first,
        ] + [page.get_by_text(hint).first for hint in COMPANY_TEXT_HINTS]
        for probe in probes:
            try:
                if await probe.is_visible():
                    return "in"
            except Exception:
                continue
        try:
            if await page.locator("input[type='password']").first.is_visible():
                return "out"
        except Exception:
            pass
    except Exception:
        pass
    return None


async def _context_logged_in(context) -> bool:
    for pg in list(context.pages):
        if await _page_login_state(pg) == "in":
            return True
    return False


async def verify_session_saved() -> bool:
    """Mở lại profile (ẩn), BƠM COOKIE TỪ JSON như downloader sẽ làm,
    rồi kiểm tra thật. Luôn chụp màn hình ra VERIFY_SCREENSHOT."""
    log("[verify] bắt đầu kiểm chứng (mở headless + bơm cookie)...")
    try:
        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                user_data_dir=str(PROFILE_DIR),
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
                viewport={"width": 1280, "height": 800},
            )
            await inject_saved_cookies(context)
            page = context.pages[0] if context.pages else await context.new_page()
            await page.goto(MISA_LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)
            log(f"[verify] đã vào {page.url}")
            ok: bool | None = None
            saw_password = False
            deadline = time.monotonic() + VERIFY_WAIT_SECONDS
            last_report = 0.0
            while time.monotonic() < deadline:
                state = await _page_login_state(page)
                if state == "in":
                    log("[verify] thấy marker ĐÃ đăng nhập.")
                    ok = True
                    break
                if state == "out":
                    log("[verify] thấy Ô MẬT KHẨU hiển thị — session mất.")
                    saw_password = True
                    ok = False
                    break
                if time.monotonic() - last_report > 10:
                    log(f"[verify] đang chờ, chưa kết luận được... url={page.url}")
                    last_report = time.monotonic()
                await asyncio.sleep(1)
            if ok is None:
                try:
                    body_text = (await page.locator("body").inner_text()).strip()
                except Exception:
                    body_text = ""
                ok = (not saw_password) and len(body_text) > 200
                log(f"[verify] hết {VERIFY_WAIT_SECONDS}s không marker nào — "
                    f"fallback (không thấy mật khẩu + body {len(body_text)} ký tự) → {ok}")
            try:
                await page.screenshot(path=str(VERIFY_SCREENSHOT), full_page=True)
                log(f"[verify] đã chụp màn hình -> {VERIFY_SCREENSHOT.name}")
            except Exception:
                log_exc("[verify] chụp màn hình LỖI")
            await context.close()
            log(f"[verify] KẾT QUẢ: {'CÒN session' if ok else 'MẤT session'}")
            return bool(ok)
    except Exception:
        log_exc("[verify] LỖI tổng")
        return False


async def manual_check() -> None:
    """--kiem-tra: mở Chrome HIỆN HÌNH + bơm cookie như downloader."""
    log("=== CHẾ ĐỘ --kiem-tra ===")
    popup(
        "Kiểm tra session MISA",
        "Chrome sẽ mở bằng đúng hồ sơ + cookie mà hệ thống tự động dùng.\n\n"
        "• Thấy DANH SÁCH CÔNG TY (không hỏi mật khẩu) → session CÒN.\n"
        "• Thấy Ô MẬT KHẨU → session MẤT, cần đăng nhập lại.\n\n"
        "Xem xong đóng Chrome lại.",
    )
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=False,
            args=["--start-maximized", "--disable-blink-features=AutomationControlled"],
            viewport=None,
        )
        await inject_saved_cookies(context)
        page = context.pages[0] if context.pages else await context.new_page()
        await page.goto(MISA_LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)
        log(f"[kiem-tra] đã vào {page.url} — chờ bạn xem và đóng Chrome.")
        try:
            await context.wait_for_event("close", timeout=0)
        except Exception:
            pass
        try:
            await context.close()
        except Exception:
            pass
    log("[kiem-tra] xong.")


async def main() -> None:
    log("=" * 60)
    log("=== BẮT ĐẦU ĐĂNG NHẬP MISA ===")
    popup(
        "Đăng nhập MISA",
        "Cửa sổ Chrome sẽ mở ra và TỰ ĐỘNG đăng nhập lại (không cần bạn "
        "gõ gì) — cửa sổ sẽ TỰ ĐÓNG khi xong.\n\n"
        "Chỉ khi hệ thống hiện Ô MẬT KHẨU (trường hợp hiếm, thiết bị mất "
        "trạng thái tin cậy) mới cần bạn tự đăng nhập tay (nhập OTP nếu "
        "được hỏi). Khi đó cửa sổ vẫn TỰ ĐÓNG sau khi bạn đăng nhập xong.",
    )
    # Backup file cookie đang dùng — hỏng thì khôi phục, không phá
    # session tốt đang có.
    backup = None
    if COOKIES_FILE.exists():
        try:
            backup = COOKIES_FILE.with_suffix(".backup.json")
            backup.write_bytes(COOKIES_FILE.read_bytes())
            log(f"[backup] đã backup cookie hiện tại -> {backup.name}")
        except Exception:
            log_exc("[backup] LỖI backup")
            backup = None
    else:
        log("[backup] chưa có file cookie cũ — bỏ qua backup.")
    saved_count = 0
    skip_verification_count = 0
    try:
        async with async_playwright() as p:
            try:
                context = await p.chromium.launch_persistent_context(
                    user_data_dir=str(PROFILE_DIR),
                    headless=False,
                    args=[
                        "--start-maximized",
                        "--disable-blink-features=AutomationControlled",
                    ],
                    viewport=None,
                )
                log("[launch] đã mở Chrome (profile automation).")
            except Exception as e:
                log_exc("[launch] LỖI mở Chrome")
                popup(
                    "Không mở được trình duyệt",
                    "Có thể hệ thống đang chạy tải báo cáo và giữ hồ sơ đăng nhập.\n\n"
                    "Vui lòng đợi vài phút rồi chạy lại 'Đăng nhập MISA'.\n\n"
                    f"Chi tiết kỹ thuật: {e}",
                    ICON_ERROR,
                )
                return
            page = context.pages[0] if context.pages else await context.new_page()
            await page.goto(MISA_LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)
            log(f"[nav] đã vào {page.url} — chờ khách đăng nhập "
                f"(tối đa {LOGIN_WAIT_MINUTES} phút)...")
            logged = False
            need_manual = False
            auto_login_attempted_urls: set[str] = set()
            deadline = time.monotonic() + LOGIN_WAIT_MINUTES * 60
            last_export = 0.0
            last_report = 0.0
            while time.monotonic() < deadline:
                try:
                    if len(context.pages) == 0:
                        log("[wait] khách đã tự đóng Chrome — dùng bản cookie xuất gần nhất.")
                        break
                    # (1) Tự bấm "Bỏ qua, tiếp tục làm việc" nếu đang hiện.
                    just_skipped = await try_skip_verification(context)
                    if just_skipped:
                        skip_verification_count += just_skipped
                        log(f"[xac-thuc] đã tự bấm 'Bỏ qua xác thực' "
                            f"({just_skipped} tab, tổng lần này: "
                            f"{skip_verification_count}).")
                    # (2) Tự điền định danh + bấm "Đăng nhập" nếu đang hiện
                    # form đăng nhập (không cần khách gõ gì) — thử trên MỌI
                    # tab đang mở, mỗi tab chỉ thử 1 lần cho tới khi URL đổi
                    # (tránh bấm lặp vô ích nhiều vòng liên tiếp trên cùng
                    # 1 màn hình chưa kịp chuyển).
                    if not logged and not need_manual:
                        for pg in list(context.pages):
                            key = f"{id(pg)}::{pg.url}"
                            if key in auto_login_attempted_urls:
                                continue
                            result = await try_auto_login(pg)
                            if result == "clicked_login":
                                auto_login_attempted_urls.add(key)
                                log("[auto-login] đã tự điền định danh + bấm 'Đăng nhập'.")
                            elif result == "need_password":
                                if not need_manual:
                                    need_manual = True
                                    log("[auto-login] gặp Ô MẬT KHẨU thật — "
                                        "thiết bị hết trạng thái tin cậy, "
                                        "cần khách tự đăng nhập tay (không "
                                        "tự động tiếp được).")
                    if await _context_logged_in(context):
                        log("[wait] PHÁT HIỆN đã đăng nhập!")
                        logged = True
                        break
                    if time.monotonic() - last_export > 4:
                        try:
                            saved_count = await export_cookies(context)
                        except Exception:
                            log_exc("[cookie] export định kỳ LỖI")
                        last_export = time.monotonic()
                    if time.monotonic() - last_report > 10:
                        urls = [pg.url for pg in context.pages]
                        log(f"[wait] chưa phát hiện đăng nhập... tabs={urls}")
                        last_report = time.monotonic()
                except Exception:
                    log_exc("[wait] LỖI trong vòng chờ — thoát vòng")
                    break
                await asyncio.sleep(1.5)
            else:
                log(f"[wait] HẾT {LOGIN_WAIT_MINUTES} phút không phát hiện đăng nhập.")
            if logged:
                await asyncio.sleep(2)  # cho MISA set nốt cookie sau redirect
                try:
                    saved_count = await export_cookies(context)
                    log(f"[cookie] đã xuất bản CHUẨN: {saved_count} cookie -> {COOKIES_FILE.name}")
                except Exception:
                    log_exc("[cookie] export bản chuẩn LỖI")
                    saved_count = 0
            log("[close] đang đóng Chrome (graceful)...")
            try:
                await context.close()
                log("[close] đã đóng.")
            except Exception:
                log_exc("[close] LỖI đóng (thường vô hại nếu khách đã tự đóng)")
    except Exception:
        log_exc("[main] LỖI tổng luồng đăng nhập")
    ok = saved_count > 0 and await verify_session_saved()
    log(f"[kết luận] saved_count={saved_count}, "
        f"skip_verification_count={skip_verification_count}, "
        f"verify={'OK' if ok else 'FAIL'}")
    if not ok and backup is not None:
        try:
            COOKIES_FILE.write_bytes(backup.read_bytes())
            log("[restore] lần này hỏng — đã khôi phục bản cookie cũ.")
        except Exception:
            log_exc("[restore] LỖI khôi phục")
    if ok:
        popup(
            "Đăng nhập thành công ✓",
            f"Hệ thống đã lưu {saved_count} cookie và kiểm chứng đăng nhập.\n\n"
            f"{SCHEDULE_NOTE}\n\n"
            "Bạn không cần làm gì thêm.",
        )
    else:
        popup(
            "Chưa hoàn tất ✗",
            "Hệ thống CHƯA xác nhận được đăng nhập.\n\n"
            "Vui lòng chạy lại 'Đăng nhập MISA', đăng nhập xong và ĐỢI\n"
            "cửa sổ TỰ đóng (không tự tắt Chrome giữa chừng).\n\n"
            "(Kỹ thuật viên: xem dang_nhap_misa.log và kiem_chung_misa.png\n"
            "trong D:\\Source\\Automation.)",
            ICON_WARN,
        )
    log("=== KẾT THÚC ===")


if __name__ == "__main__":
    if "--kiem-tra" in sys.argv:
        asyncio.run(manual_check())
    else:
        asyncio.run(main())