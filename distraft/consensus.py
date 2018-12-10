import json
import functools
import random
import asyncio
import logging
from networking import PeerProtocol
from timers import EventTimer
from log import PersistentLog

logger = logging.getLogger(__name__)

# Configuration:
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


def validate_commit_index(func):
        """Apply to State Machine everything up to commit index"""

        @functools.wraps(func)
        def wrapped(self, *args, **kwargs):
            for not_applied in range(self.log.last_applied + 1, self.log.commit_index + 1):
                self.log.last_applied += 1

                try:
                    self.apply_future.set_result(not_applied)
                except (asyncio.futures.InvalidStateError, AttributeError):
                    pass

            return func(self, *args, **kwargs)
        return wrapped


class Raft:

    def __init__(self, host, udp_port, tcp_port, loop, members=None, leader=False):
        """Creates a Raft consensus server to be used however needed

        args:
            - host: hostname / IP for this server
            - host: port number for this server
            - loop: asyncio loop object
            - members: list of members dicrionary, like:
                [
                    {"host": "127.0.0.1", "udp_port": 8001, "tcp_port": 8001, "leader": False}
                    {"host": "127.0.0.1", "udp_port": 8001, "tcp_port": 8001, "leader": False}
                    ...
                ]
            - leader: is this server the leader?
        """
        self.apply_future = None
        self.udp_address = NetAddress(host, udp_port)
        self.tcp_address = NetAddress(host, tcp_port)
        self.id = self.udp_address.full_address
        self.loop = loop
        self.queue = asyncio.Queue(loop=self.loop)
        self._leader = leader
        self.last_counter = 0
        self.term = 0
        self.request_id = 0
        self.members = ([
            {
                "udp_address": NetAddress(m['host'], m['udp_port']),
                "tcp_Address": NetAddress(m['host'], m['tcp_port']),
                "leader": m['leader']
            }
            for m in members
        ] if members else [])

        # look for the leader and get his info
        if self.leader:
            self._leader_address = self.tcp_address.full_address
        else:
            for m in self.members:
                if m['leader']:
                    self._leader_address = m['tcp_Address'].full_address

        # Initialize log
        self.log = PersistentLog(node_id=self.id,
                                 log_path='/Users/itomaldonado/git/distraft/logs',
                                 reset_log=RESET_LOGS)
        self.last_index = self.log.last_log_index
        self.log.next_index = {
            m['udp_address'].full_address: self.log.last_log_index + 1 for m in self.members
        }

        self.log.match_index = {
            m['udp_address'].full_address: 0 for m in self.members
        }

        # every 50-100 ms (uniformly distributed)
        self.heartbeat_timer = EventTimer(self.generate_timeout, self.heartbeat)

    def generate_timeout(self):
        return random.uniform(HEARTBEAT_TIMEOUT_LOWER, 2*HEARTBEAT_TIMEOUT_LOWER)

    def network_request_handler(self, data):
        getattr(self.state, 'handle_request_{}'.format(data['type']))(data)

    def handle_network_message(self, data):
        logger.info(f'server_id: {self.id}, Received: {json.dumps(data)}')

        # It's always one entry for now
        for entry in data['entries']:
            self.log.write(entry['term'], entry['command'])

        # TODO: handle responses

    def handle_request_append_entries(self, data):
        logger.info(f'append_entries_request --> server_id: {self.id}, Received: {json.dumps(data)}')

        # It's always one entry for now
        for entry in data['entries']:
            self.log.write(entry['term'], entry['command'])

        response = {
            'type': 'append_entries_response',
            'term': self.storage.term,
            'success': True,

            'request_id': data['request_id']
        }
        asyncio.ensure_future(self.state.send(response, data['sender']), loop=self.loop)

    @validate_commit_index
    def handle_request_append_entries_response(self, data):
        logger.info(
            f'append_entries_response --> server_id: {self.id}, Received: {json.dumps(data)}'
        )

    def heartbeat(self):
        self.last_counter += 1
        self.request_id += 1
        asyncio.ensure_future(self.append_entries(), loop=self.loop)

    async def append_entries(self):
        for m in self.members:
            data = {
                'type': 'append_entries',
                'term': self.term,
                'leader_id': self.id,
                'commit_index': self.log.commit_index,
                'request_id': self.last_counter,
                'entries': []
            }

            next_index = self.log.next_index[m['udp_address'].full_address]
            prev_index = next_index - 1

            if self.log.last_log_index >= next_index:
                data['entries'] = [self.log[next_index]]
                self.log.next_index[m['udp_address'].full_address] += 1

            data.update({
                'prev_log_index': prev_index,
                'prev_log_term': self.log[prev_index]['term'] if self.log and prev_index else 0
            })

            logger.info(f'Sending data: {data} to {m.udp_address.full_address}')

            host = m['udp_address'].host
            port = m['udp_address'].port
            asyncio.ensure_future(
                self.send(data=data, dest_host=host, dest_port=port),
                loop=self.loop
            )

        if self.apply_future:
            self.apply_future.set_result({'ok': True})

    async def start(self):
        protocol = PeerProtocol(
            network_queue=self.queue,
            network_request_handler=self.handle_network_message,
            loop=self.loop
        )
        self.transport, _ = await asyncio.Task(
            self.loop.create_datagram_endpoint(protocol,
                                               local_addr=self.udp_address.address_touple),
            loop=self.loop
        )
        if self.leader:
            self.heartbeat_timer.start()

    def stop(self):
        if self.leader:
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

    def broadcast(self, data):
        """Sends data to all Members in cluster (members list does not contain self)"""
        for m in self.members:
            host = m['udp_address'].host
            port = m['udp_address'].port
            asyncio.ensure_future(
                self.send(data=data, dest_host=host, dest_port=port),
                loop=self.loop
            )

    @property
    def leader(self):
        return self._leader

    @property
    def leader_address(self):
        return self._leader_address

    async def execute_command(self, command):
        """Write to persistent log & send AppendEntries() RPC"""
        self.apply_future = asyncio.Future(loop=self.loop)

        logger.info(f'writing --> term:{self.term}, command: {command}')
        self.log.write(self.term, command)
        asyncio.ensure_future(self.append_entries(), loop=self.loop)

        await self.apply_future

    async def set_value(self, name, value):
        await self.execute_command({name: value})
