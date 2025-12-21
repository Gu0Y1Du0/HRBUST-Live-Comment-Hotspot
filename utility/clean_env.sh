#!/bin/bash

# --- 配置区域 ---
KAFKA_SERVER="hadoop01:9092"
REDIS_HOST="hadoop03"
# 清理的三个 Topic
TOPICS=("danmaku_raw" "danmaku_agg" "danmaku_wordcloud")

echo "=================================================="
echo "   HRBUST Monitor - 环境彻底清洗脚本"
echo "=================================================="

# 停止所有服务
echo "[1/4] 正在停止所有后台服务 (PM2)..."
pm2 stop all

# 清空Redis
echo "[2/4] 正在清空 Redis 所有数据..."
redis-cli -h $REDIS_HOST FLUSHALL
echo "   Redis 已清空。"

# 删除 Kafka Topics
echo "[3/4] 正在删除 Kafka Topics (可能需要几秒钟)..."
for topic in "${TOPICS[@]}"; do
  # 尝试删除 topic
  kafka-topics.sh --bootstrap-server $KAFKA_SERVER --delete --topic $topic 2>/dev/null

  if [ $? -eq 0 ]; then
    echo "   Topic '$topic' 删除指令已发送。"
  else
    echo "   Topic '$topic' 不存在或删除失败 (可能本来就是空的)。"
  fi
done

echo "等待 Kafka 完成清理动作 (5秒)..."
sleep 5

echo "PY  [4/5] 正在重建 Kafka Topics (防止 Flink 报错)..."
for topic in "${TOPICS[@]}"; do
  # 创建 topic，1个分区，1个副本
  kafka-topics.sh --bootstrap-server $KAFKA_SERVER --create --topic $topic --partitions 1 --replication-factor 1 2>/dev/null
  if [ $? -eq 0 ]; then
    echo "   Topic '$topic' 重建成功。"
  else
    echo "   Topic '$topic' 重建失败 (可能已存在)。"
  fi
done

# 重启服务 (服务启动时会自动重建Topic)
echo "[4/4] 正在重启服务..."
pm2 restart all

echo "=================================================="
echo "环境已重置！现在是全新的开始。"
echo "=================================================="
