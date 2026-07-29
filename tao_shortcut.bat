@echo off
chcp 65001 >nul

:: Tìm đường dẫn Desktop thật (kể cả trong OneDrive)
for /f "tokens=*" %%D in ('powershell -NoProfile -Command "[Environment]::GetFolderPath(\"Desktop\")"') do set DESKTOP=%%D

:: Tạo file .bat trên Desktop thay vì .lnk (không dùng COM, không bị lỗi tiếng Việt)
set TARGET=%DESKTOP%\Dang nhap MISA.bat
echo @echo off > "%TARGET%"
echo cd /d D:\Source\Automation >> "%TARGET%"
echo venv\Scripts\python.exe dang_nhap_misa.py >> "%TARGET%"

echo.
echo Da tao file "%TARGET%"
echo Khach hang double-click vao file do de dang nhap MISA.
pause