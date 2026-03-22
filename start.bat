@echo off
chcp 65001 >nul
echo ========================================
echo   研报下载系统
echo ========================================
echo.

cd /d C:\Projects\research_report_system

echo 检查Python环境...
python --version
if errorlevel 1 (
    echo 错误: 未找到Python，请先安装Python 3.11+
    pause
    exit /b 1
)

echo.
echo 检查虚拟环境...
if not exist "venv" (
    echo 创建虚拟环境...
    python -m venv venv
)

echo.
echo 激活虚拟环境并检查依赖...
call venv\Scripts\activate.bat

pip show fastapi >nul 2>&1
if errorlevel 1 (
    echo 安装依赖...
    pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
)

echo.
echo 启动服务...
echo.
echo 访问地址:
echo   本地: http://localhost:8080
echo   远程: http://100.64.93.19:8080
echo.
echo 按 Ctrl+C 停止服务
echo ========================================
echo.

python -m uvicorn app.main:app --host 0.0.0.0 --port 8080

pause