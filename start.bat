@echo off
chcp 65001 >nul
title Workflow Server
cd /d "%~dp0"

if not exist "%~dp0server.py" (
  echo.
  echo  [ERROR] server.py not found.
  echo  Please EXTRACT the zip first: right-click RequestTracker.zip - "Extract All..."
  echo  then open the extracted RequestTracker folder and double-click start.bat there.
  echo.
  pause
  exit /b 1
)

set "PY="
py -3 --version >nul 2>&1 && set "PY=py -3"
if not defined PY (
  python --version >nul 2>&1 && set "PY=python"
)
if not defined PY (
  echo.
  echo  [ERROR] Python is not installed, or not added to PATH.
  echo  1. Download Python from https://www.python.org/downloads/
  echo  2. In the installer, TICK "Add python.exe to PATH", then click "Install Now".
  echo  3. Close this window and double-click start.bat again.
  echo.
  pause
  exit /b 1
)

echo Using: %PY%
%PY% --version
%PY% -c "import sys; sys.exit(0 if sys.version_info >= (3,8) else 1)"
if errorlevel 1 (
  echo  [ERROR] Python 3.8 or newer is required. Please install the latest Python from python.org
  pause
  exit /b 1
)

%PY% server.py %*
echo.
echo  Server stopped. If you see an error above, take a screenshot and send it.
pause
