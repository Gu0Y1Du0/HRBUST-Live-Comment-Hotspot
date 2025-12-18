import asyncio
from . import packet_util
from .async_message_consumer import AsyncMessageConsumer
import logging


class AsyncMessageWorker:
    def __init__(self, socket, room_id):
        self.need_stop = False
        self.socket = socket
        self.room_id = room_id
        # 使用异步队列
        self.msg_queue = asyncio.Queue()
        self.message_consumer = AsyncMessageConsumer(self.msg_queue)

    def add_handler(self, msg_type, handler):
        self.message_consumer.add_handler(msg_type, handler)

    def set_stop(self, need_stop=True):
        self.need_stop = need_stop
        self.message_consumer.set_stop(need_stop)

    async def prepare(self):
        # 启动消费者协程
        asyncio.create_task(self.message_consumer.run())
        await self.enter_room()

    async def enter_room(self):
        # 登录请求
        ori_str = packet_util.assemble_login_str(self.room_id)
        data = packet_util.assemble_transfer_data(ori_str)
        await self.socket.send(data)
        # 加入房间组
        ori_str = packet_util.assemble_join_group_str(self.room_id)
        data = packet_util.assemble_transfer_data(ori_str)
        await self.socket.send(data)

    async def run(self):
        await self.prepare()
        while not self.need_stop:
            try:
                # 接收包大小
                packet_size = await self.socket.receive(4)
                if packet_size is None:
                    logging.warning("Socket closed, attempting to reconnect")
                    await self.socket.connect()
                    await self.enter_room()
                    continue

                packet_size = int.from_bytes(packet_size, byteorder='little')
                # 接收数据包
                data = await self.socket.receive(packet_size)
                if data is None:
                    logging.warning("Socket closed, attempting to reconnect")
                    await self.socket.connect()
                    await self.enter_room()
                    continue

                # 将数据放入队列
                await self.msg_queue.put(data)

            except Exception as e:
                logging.error(f"Message worker error: {e}")
                # 出错时短暂休眠避免忙循环
                await asyncio.sleep(1)
