import json
import redis
import logging
import time
import os
import argparse
from pyflink.common.time import Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction, ReduceFunction
from dotenv import load_dotenv

# --- 配置 ---
current_dir = os.path.dirname(os.path.dirname(__file__))
env_path = os.path.join(current_dir, "../.env")
load_dotenv(env_path)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "hadoop01:9092")
# Kafka配置
INPUT_TOPIC = "danmaku_raw"        # 原始弹幕主题
OUTPUT_TOPIC = "danmaku_hot"      # 输出最火弹幕主题
CONSUMER_GROUP_ID = "flink-hot-danmaku-group"  # 独立消费组ID

# 窗口配置
WINDOW_SIZE = 30    # 滑动窗口大小（秒）
SLIDE_INTERVAL = 5  # 窗口滑动间隔（秒）
# 弹幕过滤配置
MIN_DANMAKU_LENGTH = 2  # 最小弹幕长度
TOP_N = 3               # 取前N条最火弹幕

# 初始化日志（与bili-collector风格一致）
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("flink-danmaku-+1")

def get_args():
    parser = argparse.ArgumentParser(description="Flink Hot Danmaku Analysis")
    parser.add_argument(
        "--window-size", type=int, default=WINDOW_SIZE,
        help=f"Sliding window size in seconds (default: {WINDOW_SIZE})"
    )
    parser.add_argument(
        "--slide-interval", type=int, default=SLIDE_INTERVAL,
        help=f"Window slide interval in seconds (default: {SLIDE_INTERVAL})"
    )
    parser.add_argument(
        "--top-n", type=int, default=TOP_N,
        help=f"Number of top danmaku to output (default: {TOP_N})"
    )
    return parser.parse_args()

# 获取命令行参数
args = get_args()
WINDOW_SIZE = args.window_size
SLIDE_INTERVAL = args.slide_interval
TOP_N = args.top_n

def extract_danmaku(value):
    """提取完整弹幕内容和房间ID，增加完善的异常处理"""
    try:
        data = json.loads(value)
        room_id = str(data.get("room_id", "unknown"))
        content = data.get("content", "").strip()
        
        # 过滤过短/空弹幕
        if len(content) >= MIN_DANMAKU_LENGTH:
            return [(room_id, content)]
        return []
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败，数据内容: {value[:100]} (截断)，异常: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"处理弹幕数据异常，数据内容: {value[:100]} (截断)，异常: {str(e)}")
        return []

class DanmakuTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            return int(json.loads(value)["ts"])
        except (json.JSONDecodeError, KeyError, ValueError):
            return int(time.time() * 1000)


class DanmakuCountReduceFunction(ReduceFunction):
    """预聚合：累加同一条弹幕的计数"""
    def reduce(self, value1, value2):  # pyright: ignore
        # value1 = ((room_id, content), count1)
        # value2 = ((room_id, content), count2)
        key = value1[0]  # (room_id, content)
        total_count = value1[1] + value2[1]
        return (key, total_count)

class TopNDanmakuFunction(ProcessWindowFunction):
    """窗口处理函数：对预聚合后的结果排序，取TopN最火弹幕"""
    def __init__(self, top_n):
        self.top_n = top_n

    def process(self, key, context, elements):
        """
        key: room_id
        context: 窗口上下文
        elements: 预聚合后的元素，格式为((room_id, content), count)
        """
        room_id = key
        danmaku_count = {}
        
        # 整理预聚合结果
        for (_, content), count in elements:
            danmaku_count[content] = count
        
        # 按出现次数降序排序，取前N条
        sorted_danmakus = sorted(
            danmaku_count.items(),
            key=lambda x: x[1],
            reverse=True
        )[:self.top_n]
        
        # 构造输出数据
        if sorted_danmakus:
            result = {
                "room_id": room_id,
                "type": "hot_danmaku",
                "ts": int(context.window().end),  # 窗口结束时间，毫秒
                "data": [{"content": d, "count": c} for d, c in sorted_danmakus]
            }
            yield json.dumps(result, ensure_ascii=False)


if __name__ == "__main__":
    # 创建Flink流处理环境
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # 测试设为1，生产环境可根据集群调整
    
    logger.info("Flink环境初始化完成")

    # 配置Kafka Source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id(CONSUMER_GROUP_ID)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # 配置Watermark策略，处理乱序数据，安全提取时间戳
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(3))
        .with_timestamp_assigner(DanmakuTimestampAssigner())
        .with_idleness(Duration.of_seconds(5))
    )

    # 读取Kafka数据
    stream = env.from_source(
        kafka_source,
        watermark_strategy,
        "Danmaku Kafka Source"
    )

    # 数据处理流程，增加预聚合优化
    processed_stream = (
        # 提取房间ID和弹幕内容
        stream.flat_map(
            extract_danmaku,
            output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
        )
        # 转换为((room_id, content), 1)格式，用于预聚合
        .map(
            lambda x: ((x[0], x[1]), 1),
            output_type=Types.TUPLE([
                Types.TUPLE([Types.STRING(), Types.STRING()]),
                Types.INT()
            ])
        )
        # keyby按(room_id, content)分组——预聚合
        .key_by(lambda x: x[0])
        # 滑动窗口（事件时间）
        .window(SlidingEventTimeWindows.of(Time.seconds(WINDOW_SIZE), Time.seconds(SLIDE_INTERVAL)))
        # 预聚合计数
        .reduce(DanmakuCountReduceFunction())
        # keyby仅按room_id分组，用于窗口内TopN计算
        .key_by(lambda x: x[0][0])
        # 再次应用相同窗口，保证窗口范围一致
        .window(SlidingEventTimeWindows.of(Time.seconds(WINDOW_SIZE), Time.seconds(SLIDE_INTERVAL)))
        # 处理TopN逻辑
        .process(TopNDanmakuFunction(top_n=TOP_N), output_type=Types.STRING())
    )

    # 配置Kafka Sink，输出结果
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # 输出到Kafka，控制台打印
    processed_stream.sink_to(kafka_sink)
    processed_stream.print("最火弹幕输出：")

    # 执行Flink任务
    logger.info("启动Flink最火弹幕统计任务...")
    env.execute("Flink Hot Danmaku Analysis (Slide {}s/{}s)".format(WINDOW_SIZE, SLIDE_INTERVAL))

