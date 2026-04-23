@echo off
setlocal

cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
  echo Python was not found on PATH.
  echo Try running: py -m streamlit run streamlit_app.py
  pause
  exit /b 1
)

python -m streamlit --version >nul 2>nul
if errorlevel 1 (
  echo Streamlit is not installed for this Python.
  echo Run: python -m pip install -r requirements.txt
  pause
  exit /b 1
)

python -m streamlit run streamlit_app.py

endlocal
