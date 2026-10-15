@echo off
chcp 65001 >nul
title Install RapidOCR
cd /d "%~dp0"
set "PY="
py -3 --version >nul 2>&1 && set "PY=py -3"
if not defined PY (
  python --version >nul 2>&1 && set "PY=python"
)
if not defined PY (
  echo  [ERROR] Python is not installed. Install Python first ^(see start.bat^).
  pause
  exit /b 1
)
echo Installing RapidOCR (needs Internet, about 50-100 MB) ...
%PY% -m pip install --upgrade rapidocr_onnxruntime
if errorlevel 1 (
  echo Trying the newer package "rapidocr" ...
  %PY% -m pip install --upgrade rapidocr onnxruntime
)
%PY% -c "import importlib.util as u,sys; sys.exit(0 if (u.find_spec('rapidocr_onnxruntime') or u.find_spec('rapidocr')) else 1)"
if errorlevel 1 (
  echo.
  echo  [ERROR] RapidOCR was not installed. Take a screenshot of this window and send it.
) else (
  echo.
  echo  [OK] RapidOCR installed. Close the server window and run start.bat again.
)
pause
