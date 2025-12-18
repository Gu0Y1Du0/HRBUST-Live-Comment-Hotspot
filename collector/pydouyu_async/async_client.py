from .async_message_worker import AsyncMessageWorker
from .async_heartbeat_worker import AsyncHeartbeatWorker
from .async_tcpsocket import AsyncTCPSocket
import asyncio

class AsyncClient(object):
    def __init__(self, room_id=562590, heartbeat_interval=45,
                 barrage_host="danmuproxy.douyu.com",
                 barrage_port=8601):
        self.room_id = room_id
        self.heartbeat_interval = heartbeat_interval
        self.barrage_host = barrage_host
        self.barrage_port = barrage_port
        self.tcp_socket = AsyncTCPSocket(self.barrage_host, self.barrage_port)
        self.message_worker = AsyncMessageWorker(self.tcp_socket, self.room_id)
        self.heartbeat_worker = AsyncHeartbeatWorker(self.tcp_socket, self.heartbeat_interval)

    def add_handler(self, msg_type, handler):
        self.message_worker.add_handler(msg_type, handler)

    def set_heartbeat_interval(self, heartbeat_interval):
        self.heartbeat_interval = heartbeat_interval

    def refresh_object(self):
        self.tcp_socket = AsyncTCPSocket(self.barrage_host, self.barrage_port)
        self.message_worker = AsyncMessageWorker(self.tcp_socket, self.room_id)
        self.heartbeat_worker = AsyncHeartbeatWorker(self.tcp_socket, self.heartbeat_interval)

    def set_room_id(self, room_id):
        self.room_id = room_id

    async def start(self):
        await self.tcp_socket.connect()
        # 使用 asyncio.gather 并发运行消息处理和心跳任务
        await asyncio.gather(
            self.message_worker.run(),
            self.heartbeat_worker.run()
        )

    async def stop(self):
        self.message_worker.set_stop()
        self.heartbeat_worker.set_stop()
        await self.tcp_socket.close()
        self.refresh_object()
