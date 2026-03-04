@echo off
setlocal

echo ============================================================
echo  SquareGate Equity Funding Filter PyInstaller Build
echo ============================================================
echo.

REM Ensure PyInstaller is available
"C:\Users\joshm\AppData\Local\Programs\Python\Python312\python.exe" -m PyInstaller --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] PyInstaller not found. Install with: pip install pyinstaller
    pause & exit /b 1
)

REM Clean previous build
if exist build rmdir /s /q build
if exist dist  rmdir /s /q dist

echo [INFO] Building executable...
echo.

"C:\Users\joshm\AppData\Local\Programs\Python\Python312\python.exe" -m PyInstaller ^
    --onefile ^
    --windowed ^
    --name "SquareGate" ^
    --exclude-module blpapi ^
    --hidden-import pandas ^
    --hidden-import numpy ^
    --hidden-import openpyxl ^
    --hidden-import openpyxl.styles ^
    main.py

if errorlevel 1 (
    echo.
    echo [ERROR] Build failed. Check output above.
    pause & exit /b 1
)

echo.
echo ============================================================
echo  Build complete!
echo  Executable: dist\SquareGate.exe
echo ============================================================
echo.
echo NOTE: The Bloomberg C++ runtime DLLs must be present on the
echo       target machine (typically installed with Bloomberg Terminal).
echo       Copy any blpapi*.dll files from your Python site-packages
echo       into dist\ alongside SquareGate.exe if needed.
echo.
pause
