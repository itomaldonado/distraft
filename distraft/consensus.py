import asyncio
import json
import logging
import os
import random

from networking import PeerProtocol, NetAddress
from timers import EventTimer
from storage import PersistentLog, PersistentDict

logger = logging.getLogger(__name__)

# Configuration:
HEARTBEAT_TIMEOUT_LOWER = 0.150
RESET_LOGS = True
TIME_TO_RUN = 25
LOG_PATH = '/Users/mmaldonadofigueroa/git/distraft/logs'


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
        self.mem_store = {}
        self.apply_future = None
        self.udp_address = NetAddress(host, udp_port)
        self.tcp_address = NetAddress(host, tcp_port)
        self.id = self.udp_address.full_address
        self.loop = loop
        self.queue = asyncio.Queue(loop=self.loop)
        self._leader = leader
        self.term = 0
        self.request_id = 0
        self.members = [
            {
                "udp_address": NetAddress(m['host'], m['udp_port']),
                "tcp_Address": NetAddress(m['host'], m['tcp_port']),
                "leader": m['leader']
            }
            for m in members] if members else []
        logger.debug(f'{self.id} has these members: {self.members}')

        # look for the leader and get his info
        if self.leader:
            self._leader_address = self.tcp_address.full_address
        else:
            for m in self.members:
                if m['leader']:
                    self._leader_address = m['tcp_Address'].full_address

        # Initialize log
        self.log = PersistentLog(node_id=self.id,
                                 log_path=LOG_PATH,
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
        to = random.uniform(HEARTBEAT_TIMEOUT_LOWER, 2*HEARTBEAT_TIMEOUT_LOWER)
        logger.debug(f'Generating timeout of: {to} seconds.')
        return to

    def network_request_handler(self, data):
        logger.debug(f'{self.id} received message of type: {data["type"]}')
        getattr(self, 'handle_request_{}'.format(data['type']))(data)

    def handle_network_message(self, data):
        logger.info(f'server_id: {self.id}, Received: {json.dumps(data)}')

        # It's always one entry for now
        for entry in data['entries']:
            self.__write_log(entry['term'], entry['command'])

        # TODO: handle responses

    def handle_request_append_entries(self, data):
        logger.info(f'append_entries_request -> server_id: {self.id}, Received: {json.dumps(data)}')

        # It's always one entry for now
        for entry in data['entries']:
            self.log.write(entry['term'], entry['command'])

        response = {
            'type': 'append_entries_response',
            'term': self.term,
            'success': True,
            'request_id': data['request_id']
        }
        asyncio.ensure_future(
            self.send(
                response,
                dest_host=data['sender'][0],
                dest_port=data['sender'][1]
            ),
            loop=self.loop
        )

    def handle_request_append_entries_response(self, data):
        logger.info(
            f'append_entries_response --> server_id: {self.id}, Received: {json.dumps(data)}'
        )

    def heartbeat(self):
        self.request_id += 1
        logger.debug(f'Performing heartbeat() with request id: {self.request_id}')
        asyncio.ensure_future(self.append_entries(), loop=self.loop)

    async def append_entries(self):
        logger.debug(f'sending append_entries RPC to {len(self.members)} members')
        for m in self.members:
            logger.debug(f'sending to member: {m}')
            data = {
                'type': 'append_entries',
                'term': self.term,
                'leader_id': self.id,
                'commit_index': self.log.commit_index,
                'request_id': self.request_id,
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

            logger.info(f'Sending data: {data} to {m["udp_address"].full_address}')

            host = m['udp_address'].host
            port = m['udp_address'].port
            asyncio.ensure_future(
                self.send(data=data, dest_host=host, dest_port=port),
                loop=self.loop
            )

        try:
            self.apply_future.set_result({'ok': True})
        except (asyncio.futures.InvalidStateError, AttributeError):
            pass

    async def start(self):
        protocol = PeerProtocol(
            network_queue=self.queue,
            network_request_handler=self.network_request_handler,
            loop=self.loop
        )
        self.transport, _ = await asyncio.Task(
            self.loop.create_datagram_endpoint(protocol,
                                               local_addr=self.udp_address.address_touple),
            loop=self.loop
        )
        if self.leader:
            logger.info(f'I am server {self.id} and I am the leader.')
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

    def __write_log(self, term, command={}):
        logger.info(f'{self.id} log: writing --> term:{self.term}, command: {command}')
        self.log.write(self.term, command)

    async def execute_command(self, command):
        """Write to persistent log & send AppendEntries() RPC"""
        self.apply_future = asyncio.Future(loop=self.loop)
        self.__write_log(self.term, command)
        asyncio.ensure_future(self.append_entries(), loop=self.loop)

        await self.apply_future

    async def set_value(self, key, value):
        await self.execute_command({key: value})
        self.mem_store[key] = value

    async def get_value(self, key):
        return self.mem_store.get(key, None)


class StateMachine(PersistentDict):
    """Raft Replicated State Machine — a persistent dictionary"""

    def __init__(self, node_id, data_path=None):
        self.node_id = node_id
        if not data_path:
            data_path = '/var/lib/distraft'
            self.data_filename = os.path.join(data_path, f'{self.node_id}.data')
        super().__init__(self.data_filename)

    def commit(self, command):
        """Commit a command to State Machine"""
        self.update(command)
