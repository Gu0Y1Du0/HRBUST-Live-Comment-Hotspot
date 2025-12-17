from pyflink.common.time import Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema,
    KafkaSource,
)
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.datastream.connectors.kafka import KafkaSink

import json
import redis
import logging


def parse_json(value):
    """
    解析传过来的json值
    """
    data = json.loads(value)
    return (data["room_id"], 1, data["ts"])


env = StreamExecutionEnvironment.get_execution_environment()
# the number of task slot
env.set_parallelism(1)

# 设置Kafka源
kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers("hadoop01:9092")
    .set_topics("danmaku_raw")
    .set_group_id("flink-danmaku-count")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

watermark_strategy = (
    WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(3))
    .with_timestamp_assigner(lambda event, ts: int(json.loads(event)["ts"]))  # pyright:ignore
    .with_idleness(Duration.of_seconds(5))
)

stream = env.from_source(
    kafka_source,
    watermark_strategy,
    "Kafka Source",
)


# 清洗数据+指定类型
def parse_and_count(value):
    try:
        data = json.loads(value)
        # 返回房间号，计数加1
        # 假设room_id是整数，room_id大的话改用 Types.LONG()
        return str(data.get("room_id", "unknown")), 1
    except Exception:
        return "error_room", 0


stream.map(lambda x: f"Raw: {x}").print


# # 在 map 之前加调试
# def debug_time(value):
#     data = json.loads(value)
#     ts = data["ts"]
#     import time
#
#     current = int(time.time() * 1000)
#     # 打印：数据时间&Flink机器当前时间
#     print(f"DataTS: {ts} | SystemTS: {current} | Diff: {current - ts}ms")
#     return value
#
#
# stream.map(debug_time).print()

mapped_stream = stream.map(
    parse_and_count, output_type=Types.TUPLE([Types.STRING(), Types.INT()])
)

mapped_stream.map(lambda x: f"Mapped: {x}").print()

# 分组和窗口聚合
windowed = (
    # 把这里的reduce思路改成滑动窗口
    mapped_stream.key_by(lambda x: x[0])
    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
    .reduce(
        lambda a, b: (a[0], a[1] + b[1]),  # 累加逻辑依然
        output_type=Types.TUPLE([Types.STRING(), Types.INT()]),
    )
)

# SimpleStringSchema只接受字符串，我们发送到kafka需要转换json
agg_json_stream = windowed.map(
    lambda x: json.dumps({"room_id": x[0], "count": x[1], "type": "agg"}),
    output_type=Types.STRING(),
)

# 定义序列化容器
record_serializer = (
    KafkaRecordSerializationSchema.builder()
    .set_topic("danmaku_agg")
    .set_value_serialization_schema(SimpleStringSchema())
    .build()
)

# 暂时输出到kafka，后面改出到redis
kafka_sink = (
    KafkaSink.builder()
    .set_bootstrap_servers("hadoop01:9092")
    .set_record_serializer(record_serializer=record_serializer)
    .build()
)

agg_json_stream.sink_to(kafka_sink)

agg_json_stream.map(lambda x: f"Result: {x}").print()

env.execute("Danmaku Count Per Room (10s)")
