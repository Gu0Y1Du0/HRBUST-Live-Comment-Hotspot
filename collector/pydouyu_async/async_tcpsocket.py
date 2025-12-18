import asyncio
import logging

class AsyncTCPSocket(object):
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self.closed = True

    async def send(self, data):
        if self.closed or not self.writer:
            return
        try:
            self.writer.write(data)
            await self.writer.drain()
        except Exception as e:
            await self.close()
            logging.warning("Socket send failed. Exception: %s" % e)

    async def close(self):
        if self.writer and not self.closed:
            self.writer.close()
            await self.writer.wait_closed()
            self.closed = True

    async def connect(self):
        # 添加重连机制
        while True:
            try:
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
                self.closed = False
                break
            except Exception as e:
                logging.warning("Socket connect failed with %s:%d. Exception: %s"
                                % (self.host, self.port, e))
                logging.warning("Try reconnect in 5 seconds")
                await asyncio.sleep(5)
                continue

    async def receive(self, target_size: int):
        if self.closed:
            return None
        try:
            data = await self.reader.readexactly(target_size)
            return data
        except asyncio.IncompleteReadError:
            await self.close()
            return None
        except Exception as e:
            await self.close()
            logging.warning("Socket recv failed. Exception: %s" % e)
            return None
