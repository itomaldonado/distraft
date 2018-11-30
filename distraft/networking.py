import json
import asyncio
import logging
import msgpack

logger = logging.getLogger(__name__)


class PeerProtocol(asyncio.DatagramProtocol):
    SERIALIZER = True

    def __init__(self, network_queue, network_request_handler, loop):
        self.network_queue = network_queue
        self.network_request_handler = network_request_handler
        self.loop = loop

    def __call__(self):
        return self

    def __pack(self, data):
        if self.SERIALIZER:
            return msgpack.packb(data, use_bin_type=True)
        else:
            return json.dumps(data).encode()

    def __unpack(self, data):
        if self.SERIALIZER:
            return msgpack.unpackb(data, use_list=True, encoding='utf-8')
        else:
            decoded = data.decode() if isinstance(data, bytes) else data
            return json.loads(decoded)

    async def start(self):
        while not self.transport.is_closing():
            request = await self.network_queue.get()
            data = self.__pack(request['data'])
            self.transport.sendto(data, request['destination'])

    def connection_made(self, transport):
        self.transport = transport
        asyncio.ensure_future(self.start(), loop=self.loop)

    def datagram_received(self, data, sender):
        data = self.__unpack(data)
        data.update({'sender': sender})
        self.network_request_handler(data)

    def error_received(self, exc):
        logger.error(f'Error received {exc}')

    def connection_lost(self, exc):
        logger.error(f'Connection lost {exc}')
