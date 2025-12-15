from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common import WatermarkStrategy, Duration, watermark_strategy
from pyflink.datastream.window import TumblingEventTimeWindows

import json


def parse_json(value):
    """
    解析传过来的json值
    """
    data = json.loads(value)
    return (data["room_id"], 1, data["ts"])


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(3)

# 设置Kafka源
kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers("hadoop01:9092")
    .set_topics("danmaku_raw")
    .set_group_id("flink-danmaku-count")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
    Duration.of_seconds(3)
).with_timestamp_assigner(lambda event, ts: int(json.loads(event)["ts"]))  # pyright: ignore

stream = env.from_source(
    kafka_source,
    WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(3)
    ).with_timestamp_assigner(lambda event, ts: json.loads(event)["ts"]),
    "Kafka Source",
)
