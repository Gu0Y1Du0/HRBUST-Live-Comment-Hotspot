# 创建 danmaku_raw (原始数据)
kafka-topics.sh --bootstrap-server hadoop01:9092 --create --topic danmaku_raw --partitions 1 --replication-factor 1

# 创建 danmaku_wordcloud (词云数据)
kafka-topics.sh --bootstrap-server hadoop01:9092 --create --topic danmaku_wordcloud --partitions 1 --replication-factor 1

# 创建 danmaku_agg (聚合数据)
kafka-topics.sh --bootstrap-server hadoop01:9092 --create --topic danmaku_agg --partitions 1 --replication-factor 1
