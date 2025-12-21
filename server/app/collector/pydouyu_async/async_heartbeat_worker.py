import asyncio
from . import packet_util


class AsyncHeartbeatWorker:
    def __init__(self, socket, heartbeat_interval=45):
        self.need_stop = False
        self.socket = socket
        self.heartbeat_interval = heartbeat_interval

    def set_stop(self, need_stop=True):
        self.need_stop = need_stop

    async def run(self):
        while not self.need_stop:
            # 组装心跳包
            ori_str = packet_util.assemble_heartbeat_str()
            data = packet_util.assemble_transfer_data(ori_str)
            # 异步发送心跳包
            await self.socket.send(data)
            # 异步睡眠
            await asyncio.sleep(self.heartbeat_interval)
