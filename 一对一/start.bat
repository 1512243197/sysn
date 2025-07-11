@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

:: 设置变量
set CONFIG=config.json
set EXE=sync_node_full.exe

:: 切换到脚本目录
cd /d %~dp0

:: 检查配置文件
if not exist "%CONFIG%" (
    echo ❌ 配置文件不存在: %CONFIG%
    pause
    exit /b 1
)

:: 提取端口号
for /f "usebackq tokens=*" %%p in (`powershell -NoProfile -Command ^
    "try { (Get-Content -Raw '%CONFIG%' | ConvertFrom-Json).my_port } catch { '' }"`) do (
    set PORT=%%p
)

if not defined PORT (
    echo ❌ 无法读取端口号，请检查 config.json
    pause
    exit /b 1
)

echo ✅ 读取到端口号: %PORT%

:: 删除已有规则（若存在）
echo 🔄 正在清理旧防火墙规则...
netsh advfirewall firewall delete rule name="SyncToolAllow%PORT%" >nul 2>nul

:: 添加防火墙入站规则（适用于所有配置文件）
echo 🔒 添加防火墙规则 SyncToolAllow%PORT%（入站允许，TCP）...
netsh advfirewall firewall add rule name="SyncToolAllow%PORT%" ^
    dir=in action=allow protocol=TCP localport=%PORT% profile=any

:: 启动同步程序
echo 🚀 启动同步程序...
start "" "%~dp0%EXE%" "%CONFIG%"

echo ✅ 已启动完成，监听端口 %PORT%
echo 📌 如果访问仍异常，请手动检查:
echo     - 防火墙是否被企业策略强制覆盖
echo     - 当前连接网络类型是否为“专用”或“公用”

pause
