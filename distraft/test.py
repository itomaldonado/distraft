import json
import random
import asyncio
import logging
from networking import PeerProtocol
from timers import EventTimer
from log import PersistentLog

logger = logging.getLogger(__name__)

# Configuration:
NUM_SERVERS = 5
HEARTBEAT_TIMEOUT_LOWER = 0.150
RESET_LOGS = True

TIME_TO_RUN = 25


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
        self.id = self.address.full_address
        self.loop = loop
        self.queue = asyncio.Queue(loop=self.loop)
        self.log = PersistentLog(node_id=self.id,
                                 log_path='/Users/itomaldonado/git/distraft/logs',
                                 reset_log=RESET_LOGS)
        self.last_counter = 0
        self.last_receiver = None

        # every 50-100 ms (uniformly distributed)
        self.heartbeat_timer = EventTimer(self.generate_timeout, self.send_data)

    def generate_timeout(self):
        return random.uniform(HEARTBEAT_TIMEOUT_LOWER, 2*HEARTBEAT_TIMEOUT_LOWER)

    def handle_network_message(self, data):
        logger.info(f'server_id: {self.id}, Received: {json.dumps(data)}')
        self.log.write(term=data['counter'], command=data['msg'])
        self.last_receiver = data['sender']
        if not self.heartbeat_timer.is_active:
            self.heartbeat_timer.start()

    def send_data(self):
        if self.last_receiver:
            self.last_counter += 1
            asyncio.ensure_future(self.send(
                data={'msg': self.address.full_address, 'counter': self.last_counter},
                dest_host=self.last_receiver[0], dest_port=self.last_receiver[1]))

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
        self.heartbeat_timer.stop()
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


def main_networking():
    loop = asyncio.get_event_loop()
    servers = list()

    # Create Servers
    for i in range(NUM_SERVERS):
        servers.append(Server(host='127.0.0.1', port=9000+i, loop=loop))

    # Start servers
    for server in servers:
        loop.run_until_complete(server.start())

    # send messeges to start their progress (s_n -> s_n+1)
    if len(servers) > 1:
        for i in range(len(servers) - 1):
            loop.run_until_complete(servers[i].send(data={'msg': 'test', 'counter': 1},
                                    dest_host=servers[i+1].address.host,
                                    dest_port=servers[i+1].address.port))
    # send messege (s_n -> s_0)
    loop.run_until_complete(servers[-1].send(data={'msg': 'test', 'counter': 1},
                            dest_host=servers[0].address.host,
                            dest_port=servers[0].address.port))

    # loop for a while to allow for messages
    loop.run_until_complete(asyncio.sleep(TIME_TO_RUN, loop=loop))

    # stop servers:
    for server in servers:
        server.stop()


def _setup_logging(debug):
    fmt = '[%(asctime)s]: %(levelname)s %(message)s'
    loglevel = logging.INFO
    if debug:
        loglevel = logging.DEBUG
    logging.basicConfig(format=fmt, level=loglevel)


if __name__ == '__main__':
    _setup_logging(True)
    main_networking()
