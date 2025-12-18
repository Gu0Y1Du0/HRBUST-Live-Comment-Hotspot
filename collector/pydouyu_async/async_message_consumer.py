import asyncio
import logging
from . import packet_util


class AsyncMessageConsumer:
    def __init__(self, msg_queue):
        self.need_stop = False
        # 应使用asyncio.Queue
        self.msg_queue = msg_queue
        self.handlers = {}

    def add_handler(self, msg_type, handler):
        if msg_type not in self.handlers:
            self.handlers[msg_type] = []
        self.handlers[msg_type].append(handler)

    def set_stop(self, need_stop=True):
        self.need_stop = need_stop

    async def run(self):
        while not self.need_stop:
            try:
                # 异步获取消息
                data = await self.msg_queue.get()
                # 解析消息
                ori_str = packet_util.extract_str_from_data(data)
                msg = packet_util.parse_str_to_dict(ori_str)
                # 处理消息
                try:
                    msg_type = msg['type']
                    if msg_type in self.handlers:
                        for handler in self.handlers[msg_type]:
                            # 根据handler是否为协程函数决定调用方式
                            if asyncio.iscoroutinefunction(handler):
                                await handler(msg)
                            else:
                                handler(msg)
                except Exception as e:
                    logging.warning("Invalid msg received. Exception: %s" % e)
                # 标记任务完成
                self.msg_queue.task_done()

            except Exception as e:
                logging.error("Message consumer error: %s" % e)
                continue
