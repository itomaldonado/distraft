import json
import asyncio
import logging
from networking import PeerProtocol

logger = logging.getLogger(__name__)


class NetAddress:
    def __init__(self, host, port):
        self._host = host
        self._port = port

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def full_address(self):
        return f'{self._host}:{self._port}'

    @property
    def address_touple(self):
        return self._host, self._port


class Server:

    def __init__(self, host, port, loop):
        self.address = NetAddress(host, port)
        self.loop = loop
        self.queue = asyncio.Queue(loop=self.loop)

    def handle_network_message(self, data):
        logger.info(f'Received: {json.dumps(data)}')
        asyncio.ensure_future(self.send(
                              data={'msg': self.address.full_address, 'counter': data['counter']+1},
                              dest_host=data['sender'][0], dest_port=data['sender'][1]))

    async def start(self):
        protocol = PeerProtocol(
            network_queue=self.queue,
            network_request_handler=self.handle_network_message,
            loop=self.loop
        )
        self.transport, _ = await asyncio.Task(
            self.loop.create_datagram_endpoint(protocol, local_addr=self.address.address_touple),
            loop=self.loop
        )

    def stop(self):
        self.transport.close()

    async def send(self, data, dest_host, dest_port):
        """Sends data to destination Node
        Args:
            data — serializable object
            destination — <str> '127.0.0.1:8000' or <tuple> (127.0.0.1, 8000)
        """
        destination = NetAddress(dest_host, dest_port)
        await self.queue.put({
            'data': data,
            'destination': destination.address_touple
        })


def main():
    loop = asyncio.get_event_loop()
    s1 = Server(host='127.0.0.1', port=8000, loop=loop)
    s2 = Server(host='127.0.0.1', port=8001, loop=loop)

    loop.run_until_complete(s1.start())
    loop.run_until_complete(s2.start())

    loop.run_until_complete(s1.send(data={'msg': 'test', 'counter': 1},
                            dest_host=s2.address.host, dest_port=s2.address.port))

    loop.run_until_complete(asyncio.sleep(5, loop=loop))

    s1.stop()
    s2.stop()


def _setup_logging(debug):
    fmt = '[%(asctime)s]: %(levelname)s %(message)s'
    loglevel = logging.INFO
    if debug:
        loglevel = logging.DEBUG
    logging.basicConfig(format=fmt, level=loglevel)


if __name__ == '__main__':
    _setup_logging(True)
    main()
