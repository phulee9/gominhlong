import ctypes
import shutil
from ctypes import wintypes
from pathlib import Path
from win32com.client import Dispatch


def get_desktop_dir() -> Path:
    """Lấy Desktop thật (kể cả trong OneDrive) qua SHGetKnownFolderPath —
    Unicode chuẩn, không qua console nên không lỗi encoding."""
    FOLDERID_Desktop = ctypes.c_char_p(
        b"\x3a\xcc\xbf\xb4\x2c\xdb\x4c\x42\xb0\x29\x7f\xe9\x9a\x87\xc6\x41"
    )
    # GUID {B4BFCC3A-DB2C-424C-B029-7FE99A87C641} dạng struct
    class GUID(ctypes.Structure):
        _fields_ = [("Data1", ctypes.c_uint32), ("Data2", ctypes.c_uint16),
                    ("Data3", ctypes.c_uint16), ("Data4", ctypes.c_ubyte * 8)]

    guid = GUID(0xB4BFCC3A, 0xDB2C, 0x424C,
                (ctypes.c_ubyte * 8)(0xB0, 0x29, 0x7F, 0xE9, 0x9A, 0x87, 0xC6, 0x41))
    path_ptr = ctypes.c_wchar_p()
    result = ctypes.windll.shell32.SHGetKnownFolderPath(
        ctypes.byref(guid), 0, None, ctypes.byref(path_ptr)
    )
    if result != 0:
        raise OSError(f"SHGetKnownFolderPath lỗi: {result}")
    desktop = Path(path_ptr.value)
    ctypes.windll.ole32.CoTaskMemFree(path_ptr)
    return desktop


desktop = get_desktop_dir()
print("Desktop:", desktop)  # in ra để mắt thường xác nhận đúng đường dẫn

# 1) Tạo .lnk ở đường dẫn KHÔNG dấu — COM lưu được
temp_lnk = Path(r"D:\Source\Automation") / "Dang nhap MISA.lnk"
shell = Dispatch("WScript.Shell")
shortcut = shell.CreateShortCut(str(temp_lnk))
shortcut.Targetpath = r"D:\Source\Automation\venv\Scripts\pythonw.exe"  # pythonw: không hiện console đen
shortcut.Arguments = r"D:\Source\Automation\dang_nhap_misa.py"
shortcut.WorkingDirectory = r"D:\Source\Automation"
shortcut.IconLocation = r"C:\Windows\System32\shell32.dll,13"
shortcut.Description = "Dang nhap MISA truoc khi he thong tu chay"
shortcut.save()

# 2) Copy sang Desktop bằng Python — Unicode chuẩn
dest = desktop / "Dang nhap MISA.lnk"
shutil.copy2(temp_lnk, dest)
temp_lnk.unlink()
print("Shortcut da duoc tao tai:", dest)