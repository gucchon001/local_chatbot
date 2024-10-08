@echo off
chcp 65001 > nul

:: pipの現在のバージョンを取得して必要に応じてアップデート
for /f "tokens=2" %%a in ('pip --version') do set "current_pip_version=%%a"
set "latest_pip_version=24.2"

if not "%current_pip_version%"=="%latest_pip_version%" (
    echo Updating pip to version %latest_pip_version%...
    python -m pip install --upgrade pip==%latest_pip_version%
    if errorlevel 1 (
        echo Error: Failed to update pip to version %latest_pip_version%.
        exit /b 1
    )
    echo pip updated to version %latest_pip_version% successfully.
)

:: requirements.txtが存在するか確認
if not exist requirements.txt (
    echo Error: requirements.txt file not found.
    exit /b 1
)

:: requirements.txtのハッシュ値を計算
for /f "delims=" %%a in ('certutil -hashfile requirements.txt SHA256 ^| findstr /v "hash"') do set "current_hash=%%a"

:: 前回のハッシュ値を読み込む
if exist .req_hash (
    set /p stored_hash=<.req_hash
) else (
    set "stored_hash="
)

:: ハッシュ値を比較し、必要に応じてインストールを実行
if not "%current_hash%"=="%stored_hash%" (
    echo Requirements have changed. Installing/updating libraries...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo Error: Failed to install required libraries.
        exit /b 1
    )
    echo %current_hash%>.req_hash
    echo Libraries installed/updated successfully.
) else (
    echo No changes in requirements. Skipping installation.
)

:: 引数で指定されたアプリを実行
if "%1"=="" (
    echo Error: Please specify the app file to run, e.g., app.py
    exit /b 1
)

:: IPアドレスを取得
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr "IPv4"') do (
    set ip_address=%%a
)

:: 先頭や末尾の空白を削除
set ip_address=%ip_address: =%

:: 既に8501ポートが使用されている場合、プロセスを終了する
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8501') do (
    taskkill /F /PID %%a
)

echo Starting Streamlit application on IP: %ip_address%...

streamlit run %1 --server.address %ip_address% --server.port 8501

