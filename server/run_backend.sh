#!/bin/bash

# 显式定位到项目根目录
cd /home/hadoop/HRBUST-Live-Comment-Hotspot

# 设置 PYTHONPATH
# 将 server 目录加入到 Python 搜索路径中
# 这样代码里的 "from app import ..." 就能在 server 目录下找到 app 文件夹了
export PYTHONPATH=$PYTHONPATH:$(pwd)/server

# 强制Python输出使用UTF-8编码
export PYTHONIOENCODING=utf-8
export LANG=C.UTF-8

export PYTHONUNBUFFERED=1

# 使用模块方式启动 (-m app.main)
# 这样 __package__ 会被正确解析
./.venv/bin/python -m app.main
