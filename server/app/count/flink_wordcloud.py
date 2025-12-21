import os
import json
import logging
import jieba
from pyflink.common.time import Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from dotenv import load_dotenv

# --- 配置 ---
current_dir = os.path.dirname(os.path.dirname(__file__))
env_path = os.path.join(current_dir, "../.env")
load_dotenv(env_path)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "hadoop01:9092")

# 停用词表
STOP_WORDS = {
    "的",
    "了",
    "是",
    "我",
    "你",
    "他",
    "在",
    "就",
    "不",
    "也",
    "都",
    "这",
    "那",
    "有",
    "去",
    "啊",
    "哦",
    "嗯",
    "吧",
    "呢",
    "吗",
    "呀",
    "哈",
    "哈哈",
    "哈哈哈",
    "但是",
    "因为",
    "所以",
    "什么",
    "这个",
    "那个",
    "直播",
    "主播",
    "虽然",
    "其实",
    "看着",
    "觉得",
    "感觉",
    "还是",
}


def split_words(value):
    """
    FlatMap函数: 接收一条弹幕，返回多个 (room_id, word)
    """
    try:
        data = json.loads(value)
        room_id = str(data.get("room_id", "unknown"))
        content = data.get("content", "")

        # 结巴分词
        words = jieba.cut(content)

        # 过滤并输出
        results = []
        for w in words:
            # 词长大于1且不在停用词表中
            if len(w) > 1 and w not in STOP_WORDS:
                results.append((room_id, w))
        return results
    except Exception as e:
        print(f"抛出异常: {e}")
        return []


class TopNWordsFunction(ProcessWindowFunction):
    """
    窗口处理函数：接收一个窗口内的所有单词，统计词频并排序
    """

    def process(self, key, context, elements):
        # key是room_id
        if isinstance(key, tuple):
            room_id = key[0]  # key是tuple
        else:
            room_id = str(key)

        print(f"DEBUG: Processing window for room: {room_id}")

        # 本地统计词频
        word_count = {}
        for _, word in elements:
            word_count[word] = word_count.get(word, 0) + 1

        # 排序取前Top380
        sorted_words = sorted(
            word_count.items(), key=lambda item: item[1], reverse=True
        )[:380]

        # 转换成Echarts需要的格式
        result_list = [{"name": k, "value": v} for k, v in sorted_words]

        # 只有当有数据时才发送
        if result_list:
            output_data = {
                "room_id": room_id,
                "type": "wordcloud",
                "ts": int(context.window().end),
                "data": result_list,
            }
            yield json.dumps(output_data, ensure_ascii=False)


# Flink Job设置
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Source
kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers(BOOTSTRAP_SERVERS)
    .set_topics("danmaku_raw")
    .set_group_id("flink-wordcloud-group")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

watermark_strategy = (
    WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(3))
    .with_timestamp_assigner(lambda event, ts: int(json.loads(event)["ts"]))  # pyright: ignore
    .with_idleness(Duration.of_seconds(5))
)

stream = env.from_source(kafka_source, watermark_strategy, "Kafka Source")

# Transformation
# Json -> 分词 -> 按照房间分组 -> 滑动窗口(统计过去60s, 每5s更新一次) -> TopN计算
processed_stream = (
    stream.flat_map(
        split_words, output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    )
    .key_by(lambda x: x[0])
    .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(5)))
    .process(TopNWordsFunction(), output_type=Types.STRING())
)

# KafkaSink
# 发送到一个新的Topic: danmaku_wordcloud
# 这样就不会污染danmaku_agg
record_serializer = (
    KafkaRecordSerializationSchema.builder()
    .set_topic("danmaku_wordcloud")
    .set_value_serialization_schema(SimpleStringSchema())
    .build()
)

kafka_sink = (
    KafkaSink.builder()
    .set_bootstrap_servers(BOOTSTRAP_SERVERS)
    .set_record_serializer(record_serializer)
    .build()
)

processed_stream.sink_to(kafka_sink)

# 打印调试信息
processed_stream.print()

env.execute("Danmaku WordCloud (Slide 60s/5s)")
