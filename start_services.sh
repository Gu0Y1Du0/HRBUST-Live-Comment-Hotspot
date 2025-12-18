#!/bin/bash

# --- 配置区域 ---
# 项目根目录
PROJECT_DIR="/home/hadoop/HRBUST-Live-Comment-Hotspot"
# 虚拟环境 Python 解释器绝对路径
VENV_PYTHON="$PROJECT_DIR/.venv/bin/python"

echo "=================================================="
echo "   HRBUST Live Monitor - 服务启动脚本 (PM2)"
echo "=================================================="

# 检查 PM2 是否安装
if ! command -v pm2 &>/dev/null; then
  echo "[!] 未检测到 pm2，正在安装..."
  npm install pm2 -g
fi

# 进入项目目录
cd $PROJECT_DIR

# 启动核心服务
# (如果已经存在同名进程，先删除再启动，确保配置更新)
pm2 delete all 2>/dev/null

echo ">> 正在启动后端 API (FastAPI)..."
# 注意：这里我们让 FastAPI 监听 0.0.0.0:8000
pm2 start server/main.py \
  --name "backend-api" \
  --interpreter "$VENV_PYTHON" \
  --interpreter-args "-m uvicorn server.main:app --host 0.0.0.0 --port 8000"

echo ">> 正在启动数据桥接 (Kafka -> Redis)..."
pm2 start count/bridge_to_redis.py \
  --name "redis-bridge" \
  --interpreter "$VENV_PYTHON"

# 保存当前进程列表，以便开机自启
pm2 save

echo "=================================================="
echo "   所有服务已启动！"
echo "   使用 'pm2 monit' 查看实时日志和状态"
echo "   后端地址: http://hadoop01:8000"
echo "=================================================="
