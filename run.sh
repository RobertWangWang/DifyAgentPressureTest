#!/bin/bash
set -euo pipefail

echo "🚀 [1/6] 安装 python3-venv..."
sudo apt-get update -y
sudo apt-get install -y python3-venv

# 项目根路径
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$PROJECT_DIR/venv"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

echo "📁 [2/6] 创建虚拟环境..."
if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
  echo "✅ 虚拟环境已创建: $VENV_DIR"
else
  echo "ℹ️ 虚拟环境已存在，跳过创建。"
fi

echo "🔧 [3/6] 激活虚拟环境..."
source "$VENV_DIR/bin/activate"

echo "📦 [4/6] 安装依赖..."
pip install --upgrade pip
pip install -r "$PROJECT_DIR/requirements.txt"

echo "🚀 [5/6] 启动 FastAPI 服务（使用 nohup）..."
export PYTHONPATH="$PROJECT_DIR"
LOG_FILE="$LOG_DIR/fastapi.log"

# 使用 nohup 后台运行
nohup uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload > "$LOG_FILE" 2>&1 &

# 获取 PID
FASTAPI_PID=$!
echo "$FASTAPI_PID" > "$PROJECT_DIR/fastapi.pid"

echo "✅ FastAPI 已启动，PID: $FASTAPI_PID"
echo "📜 日志文件: $LOG_FILE"
echo "🧾 PID 文件: $PROJECT_DIR/fastapi.pid"
