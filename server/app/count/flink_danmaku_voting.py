import json
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
from pyflink.datastream.window import TumblingEventTimeWindows  # 改为滚动窗口用于固定时间统计
from pyflink.datastream.functions import ProcessWindowFunction, ReduceFunction
from dotenv import load_dotenv


# ---配置---
current_dir = os.path.dirname(os.path.dirname(__file__))
env_path = os.path.join(current_dir, "../.env")
load_dotenv(env_path)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "Master:9092")
# Kafka配置
INPUT_TOPIC = "danmaku_raw"
VOTE_OUTPUT_TOPIC = "danmaku_vote_result"  # 投票结果输出主题
CONSUMER_GROUP_ID = "flink-danmaku-vote-group"
# 投票配置
DEFAULT_WINDOW_SIZE = 30  # 投票统计时长（秒）
DEFAULT_TARGET_DANMAKUS = {     # 需要统计的目标弹幕内容/投票选项
    "0",
    "1",
}
VOTE_ROOM_FILTER = None  # 指定房间ID，None表示所有房间

# 日志配置
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("flink-danmaku-voting")

def get_args():
    parser = argparse.ArgumentParser(description="Flink Danmaku Vote Statistics")
    parser.add_argument(
        "--window-size", type=int, default=DEFAULT_WINDOW_SIZE,
        help=f"Vote window size in seconds (default: {DEFAULT_WINDOW_SIZE})"
    )
    parser.add_argument(
        "--target-danmaku", type=str, nargs='+', default=DEFAULT_TARGET_DANMAKUS,
        help="Target danmaku content for voting"
    )
    parser.add_argument(
        "--room-filter", type=str, default=None,
        help="Filter by specific room ID (default: all rooms)"
    )
    return parser.parse_args()

# 获取命令行参数
args = get_args()
VOTE_WINDOW_SIZE = args.window_size
TARGET_DANMAKUS = set(args.target_danmaku)
VOTE_ROOM_FILTER = args.room_filter

def extract_vote_danmaku(value):
    """提取符合投票条件的弹幕"""
    try:
        data = json.loads(value)
        room_id = str(data.get("room_id", "unknown"))
        content = data.get("content", "").strip()
        
        # 过滤条件：在目标弹幕列表中，且符合房间过滤条件
        if (content in TARGET_DANMAKUS and 
            (VOTE_ROOM_FILTER is None or room_id == VOTE_ROOM_FILTER)):
            return [(room_id, content)]
        return []
    except Exception as e:
        logger.error(f"处理投票弹幕异常: {str(e)}")
        return []

class DanmakuTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            return int(json.loads(value)["ts"])
        except Exception:
            return int(time.time() * 1000)

class VoteCountReduceFunction(ReduceFunction):
    """投票计数预聚合"""
    def reduce(self, value1, value2):  # pyright: ignore
        # value格式：((room_id, content), count)
        return (value1[0], value1[1] + value2[1])

class VoteResultProcessFunction(ProcessWindowFunction):
    """处理窗口内的投票结果"""
    def process(self, key, context, elements):
        room_id = key
        vote_counts = {}
        
        # 统计各投票选项的数量
        for (_, content), count in elements:
            vote_counts[content] = count
        
        # 计算总票数
        total_votes = sum(vote_counts.values())
        
        # 构造投票结果
        result = {
            "room_id": room_id,
            "type": "vote_statistics",
            "window_start": int(context.window().start),
            "window_end": int(context.window().end),
            "total_votes": total_votes,
            "details": [{"content": k, "count": v} for k, v in vote_counts.items()]
        }
        yield json.dumps(result, ensure_ascii=False)

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    logger.info("Flink弹幕投票统计环境初始化完成")

    # 配置Kafka Source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id(CONSUMER_GROUP_ID)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Watermark策略
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(3))
        .with_timestamp_assigner(DanmakuTimestampAssigner())
        .with_idleness(Duration.of_seconds(5))
    )

    # 读取数据流
    stream = env.from_source(
        kafka_source,
        watermark_strategy,
        "Danmaku Vote Source"
    )

    # 投票统计流程
    vote_stream = (
        stream.flat_map(
            extract_vote_danmaku,
            output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
        )
        .map(
            lambda x: ((x[0], x[1]), 1),  # ((房间ID, 弹幕内容), 计数1)
            output_type=Types.TUPLE([
                Types.TUPLE([Types.STRING(), Types.STRING()]),
                Types.INT()
            ])
        )
        .key_by(lambda x: x[0])  # 按房间+弹幕内容分组
        .window(TumblingEventTimeWindows.of(Time.seconds(VOTE_WINDOW_SIZE)))  # 固定时间窗口
        .reduce(VoteCountReduceFunction())
        .key_by(lambda x: x[0][0])  # 按房间分组汇总
        .window(TumblingEventTimeWindows.of(Time.seconds(VOTE_WINDOW_SIZE)))
        .process(VoteResultProcessFunction(), output_type=Types.STRING())
    )

    # 配置Kafka Sink
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(VOTE_OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # 输出结果
    vote_stream.sink_to(kafka_sink)
    vote_stream.print("弹幕投票统计结果：")

    # 执行任务
    logger.info("启动Flink弹幕投票统计任务...")
    env.execute(f"Danmaku Vote Statistics (Window {VOTE_WINDOW_SIZE}s)")

