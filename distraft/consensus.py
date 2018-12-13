import asyncio
import functools
import json
import logging
import random

from enum import Enum
from networking import PeerProtocol, NetAddress
from storage import PersistentLog, PersistentDict, PersistentStateMachine
from timers import EventTimer
from uuid import uuid1

logger = logging.getLogger(__name__)

# Configuration:
RESET_LOGS = True
TIME_TO_RUN = 25
LOG_PATH = '/Users/mmaldonadofigueroa/git/distraft/logs'
HEARTBEAT_TIMEOUT_LOWER = 0.150
MISSED_HEARTBEATS = 5
ELECTION_TIMEOUT_LOWER = 0.200
TIMEOUT_SPREAD_RATIO = 2.0


def generate_request_id():
    """Generate a new UUID request id
    returns:
        string uuid
    """
    return str(uuid1())


def validate_commit_index(fun):
        """Apply to the State Machine everything up to commit index"""
        @functools.wraps(fun)
        def wrapped(self, *args, **kwargs):
            for not_applied in range(self.log.last_applied + 1, self.log.commit_index + 1):
                self.log.last_applied += 1

                try:
                    self.raft.apply_future.set_result(not_applied)
                except (asyncio.futures.InvalidStateError, AttributeError):
                    pass

            return fun(self, *args, **kwargs)
        return wrapped


def validate_term(fun):
    """Compares current local (node's) term and request (sender's) term:

    - if current local (node's) term is older:
        update current local (node's) term and become a follower
    - if request (sender's) term is older:
        respond with {'success': False}
    """
    @functools.wraps(fun)
    def wrapped(self, data):
        if self.storage.term < data['term']:
            self.storage.update({'term': data['term']})
            if not isinstance(self, Follower):
                self.to_follower()

        if self.storage.term > data['term'] and not data['type'].endswith('_response'):
            response = {
                'success': False,
                'term': self.storage.term,
                'type': f'{data["type"]}_response',
            }
            asyncio.ensure_future(self.raft.send(response, data['sender']), loop=self.loop)
            return

        return fun(self, data)
    return wrapped


class States(Enum):
    """Enumerate the states a node can take"""
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 3


class State:
    def __init__(self, raft=None):
        """This is the base state class that all other states must implement"""
        self.raft = raft
        self.storage = self.raft.state_storage
        self.log = self.raft.log
        self.state_machine = self.raft.state_machine
        self.id = self.raft.id
        self.loop = self.raft.loop

    # remember to validate the term
    def handle_message_request_vote(self, data):
        """RequestVote RPC — used by candidates to gather votes
        args:
            term — the candidate's term
            candidate_id — the id of the candidate tjhat's requesting votes
            last_log_index — the index of the candidate's last log entry
            last_log_term — the term of the candidate's last log entry
        returns (via UDP):
            term — local term number (or updated) for candidate to see
            vote_granted — if 'True' then the local node voted for the candidate
        implementation details:
            - return 'False' if candidate's term < local (node's) term
            - if vote_granted is None, and candidate log (given by last_log_index and last_log_term)
            is *at least* as up-to-date as the local (node's) log, grant your vote

        message example:
        {
            'type': 'request_vote',
            'term': 5,
            'candidate_id': '127.0.0.1:9000',
            'last_log_index': 10,
            'last_log_term': 4
        }
        """

    # remember to validate the term
    def handle_message_request_vote_response(self, data):
        """RequestVote RPC response — see handle_message_request_vote implementation details

        message example:
        {
            'type': 'request_vote_response',
            'term': 5,
            'vote_granted': True
        }
        """

    # remember to validate the term
    def handle_message_append_entries(self, data):
        """AppendEntries RPC — replicate log entries / also used as heartbeats
        args:
            term — the sender's (hopefully a leader's) term
            leader_id — teh sender's (hopefully a leader's), used to check/update leader's details
            prev_log_index — index of log entry immediately preceding new ones
            prev_log_term — term of prev_log_index entry
            entries[] — entries to store, empty on most heartbeat (unless rebuilding follower logs)
            commit_index — the leader's commit_index
        returns (via UDP):
            term — local term number (or updated) for sender/leader to see/update
            success — 'True' if follower had an entry matching prev_log_index and prev_log_term
        implementation details:
            - reply with 'success': 'False' if sender's term < local (node's) term
            - reply with 'success': 'False' if log entry term at prev_log_index != prev_log_term
            - if an existing entry conflicts with a new one, delete entry and all following entries
            - append any new entries that are not already in the local (node's) log
            - if the sender's leader_commit > commit_index,
                set commit_index = min(leader_commit, index of last new entry)
        message example:
        {
            'type': 'append_entries',
            'term': 5,
            'leader_id': '127.0.0.1:9000',
            'commit_index': 10,
            'request_id': self.request_id,
            'entries': [],
            'prev_log_index': 10,
            'prev_log_term': 4
        }

        Note: entries *can* be empty (like in some heartbeats) of can have a list of entries like:
            [
                ...
                {"term": 3, "command": {"key": "value1"}}
                {"term": 4, "command": {"key": "value2"}}
                ...
            ]
        """
    # remember to validate the term
    def handle_message_entries_response(self, data):
        """AppendEntries RPC response — see handle_message_append_entries implementation details

        message example:
        {
            'type': 'append_entries_response',
            'term': 5,
            'success': True
        }
        """

    def stop(self):
        pass

    def start(self):
        pass

    def to_follower(self):
        self.raft.to_state(States.FOLLOWER)

    def to_candidate(self):
        self.raft.to_state(States.CANDIDATE)

    def to_leader(self):
        self.raft.to_state(States.LEADER)

    def validate_term(self, data):
        """Compares current local (node's) term and request (sender's) term:

        - if current local (node's) term is older:
            update current local (node's) term and become a follower
        - if request (sender's) term is older:
            respond with {'success': False}

        args:
            - data object received from other members

        returns:
            - True if term validation succeeds, False otherwise
        """
        if self.storage.term < data['term']:
            self.storage.update({'term': data['term']})
            if not isinstance(self, Follower):
                self.to_follower()
                return False

        if self.storage.term > data['term'] and not data['type'].endswith('_response'):
            response = {
                'success': False,
                'term': self.storage.term,
                'type': f'{data["type"]}_response',
            }
            asyncio.ensure_future(self.raft.send(response, data['sender']), loop=self.loop)
            return False

        return True

    def commit_state_to_commit_index(self):
        """Commit to the State Machine everything up to commit index"""
        for not_applied in range(self.log.last_applied + 1, self.log.commit_index + 1):
            self.log.last_applied += 1

            try:
                self.raft.apply_future.set_result(not_applied)
            except (asyncio.futures.InvalidStateError, AttributeError):
                pass

    @staticmethod
    def generate_election_timeout():
        to = random.uniform(
            float(ELECTION_TIMEOUT_LOWER),
            float(TIMEOUT_SPREAD_RATIO*ELECTION_TIMEOUT_LOWER)
        )
        logger.debug(f'Generating an eleciton timeout of: {to} seconds.')
        return to

    @staticmethod
    def generate_heartbeat_timeout():
        to = random.uniform(
            float(HEARTBEAT_TIMEOUT_LOWER),
            float(TIMEOUT_SPREAD_RATIO*HEARTBEAT_TIMEOUT_LOWER)
        )
        logger.debug(f'Generating a heartbeat timeout of: {to} seconds.')
        return to

    @staticmethod
    def generate_stepdown_timeout():
        lower = HEARTBEAT_TIMEOUT_LOWER*MISSED_HEARTBEATS
        to = random.uniform(
            float(lower),
            float(TIMEOUT_SPREAD_RATIO*lower)
        )
        logger.debug(f'Generating a heartbeat timeout of: {to} seconds.')
        return to


class Follower(State):
    """Raft server's Follower state:

    - this state will to RPCs from candidates and leaders
    — if the election_timer times out without receiving a heartbeat (AppendEntries RPC) from the
        current/later term's leader, or granting vote to candidate: transition to candidate state
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.election_timer = EventTimer(Follower.generate_election_timeout, self.to_candidate)

    def start(self):
        # (always) set vote to None
        self.storage.update({'vote': None})

        # (if unexistent) set term to zero"""
        if not self.storage.exists('term'):
            self.storage.update({'term': 0})

        # start election timer
        self.election_timer.start()

    def stop(self):
        # stop the election timer
        self.election_timer.stop()

    def handle_append_entries(self, data):
        """Handle an AppendEntries RPC, details:

        Here are some general rules to follow:
            - the remote (node's) log has to **always** be more up-to-date (term/index).
            - remote (node's) prev_log_index should match local (node's) prev_log_index
                - reply 'False' otherwise
            - or remote (node's) prev_log_index's term should match local (node's) prev_log_term
                - reply 'False' otherwise
            - if an existing entry conflicts with a new one (same index but different terms),
                delete the existing entry and all that follow it
            - write all entries from the received in message to local log
            - update indexes if necessary
            - respond with success: True
        """
        # apply to state machine and validate term
        self.commit_state_to_commit_index()
        if not self.validate_term(data):
            return

        # if term is valid and append_entries comes, set leader to the ID
        self.raft.set_leader(data['leader_id'])

        # Reply False if log doesn’t contain an entry at prev_log_index
        # whose term matches prev_log_term
        try:
            # checks for success (assume false first)
            prev_log_index = data['prev_log_index']
            success = False

            if prev_log_index <= self.log.last_log_index:
                success = True

            if self.log[prev_log_index]['term'] == data['prev_log_term']:
                success = True

            if not success:
                response = {
                    'type': 'append_entries_response',
                    'term': self.storage.term,
                    'success': success,
                    'request_id': data['request_id']
                }
                asyncio.ensure_future(self.state.send(response, data['sender']), loop=self.loop)
                return
        except IndexError:
            pass

        # If an existing entry conflicts with a new one (same index but different terms),
        # delete the existing entry and all that follow it
        new_index = data['prev_log_index'] + 1
        try:
            if self.log[new_index]['term'] != data['term'] or (
                self.log.last_log_index != prev_log_index
            ):
                self.log.delete_from(new_index)
        except IndexError:
            pass

        # write all entries received in the message to local (node's) log, in order
        for entry in data['entries']:
            self.log.write(entry['term'], entry['command'])

        # Update commit index if/when necessary
        if self.log.commit_index < data['commit_index']:
            self.log.commit_index = min(data['commit_index'], self.log.last_log_index)

        # Respond True since there was an entry matching prev_log_index and prev_log_term found
        response = {
            'type': 'append_entries_response',
            'term': self.storage.term,
            'success': True,
            'last_log_index': self.log.last_log_index,
            'request_id': data['request_id']
        }
        asyncio.ensure_future(self.state.send(response, data['sender']), loop=self.loop)

        # since we got a message, reset the election timer
        self.election_timer.reset()

    def handle_request_vote(self, data):
        """Handle a RequestVote RPC, details:

        The candidate's log has to **always** be up-to-date (equal or better data).

        Here are some rules for comparing local vs candidate logs:
            - last entries with different terms, then the log with the later term is more up-to-date
            - logs that end with the same term, then whichever log is longer is more up-to-date
        """

        # validate term
        if not self.validate_term(data):
            return

        # If you have voted for this term, don't vote again~
        if self.storage.voted is None:

            # if last log term entry are different
            if data['last_log_term'] != self.log.last_log_term:
                # larger term wins
                vote = data['last_log_term'] > self.log.last_log_term
            # if equal terms, longer log (larger index) wins
            else:
                vote = data['last_log_index'] >= self.log.last_log_index

            # construct response
            response = {
                'type': 'request_vote_response',
                'term': self.storage.term,
                'vote_granted': vote
            }

            # if going to vote for the candidate, udpate local storage
            if vote:
                self.storage.update({'voted_for': data['candidate_id']})

            # send response to sender
            asyncio.ensure_future(self.raft.send(response, data['sender']), loop=self.loop)

        # since we got a message, reset the election timer
        self.election_timer.reset()


class Candidate(State):
    """Raft server's Candidate state:

    — When transitioning to candidate state, it starts an election for a new term:
        — increment local (node's) term
        — vote for itself
        — reset election timer
        — send message of type `request_vode` (RequestVote RPC) to all other nodes
    — if the votes received constitute a majority of nodes: transition to leader
    — if a message of type `append_entries` (AppendEntries RPC) is received from a leader
        of an equal or higher term: transition to follower
    — if election timeout elapses: start new by election by becoming a candidate again
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.election_timer = EventTimer(self.generate_election_timeout, self.to_candidate)
        self.vote_count = 0

    def start(self):
        """Increment current term, vote for herself & send vote requests"""
        self.storage.update({
            'term': self.storage.term + 1,
            'voted_for': self.id
        })

        self.vote_count += 1
        self.send_request_vote()
        self.election_timer.start()

    def stop(self):
        self.election_timer.stop()

    def send_request_vote(self):
        """Send message of type request_vote (RequestVote RPC): gather votes form nodes"""
        data = {
            'type': 'request_vote',
            'term': self.storage.term,
            'candidate_id': self.id,
            'last_log_index': self.log.last_log_index,
            'last_log_term': self.log.last_log_term
        }
        self.raft.broadcast(data)

    def handle_request_vote_response(self, data):
        """Handle responses from a RequestVote RPC, details:

        - if the remote node voted for this (local) node/candidate, add to count.
        - check for majority, if we got it, transition to leader
        """

        # validate term
        if not self.validate_term(data):
            return

        if data.get('vote_granted'):
            self.vote_count += 1

            if self.raft.is_majority(self.vote_count):
                self.to_leader()

    def handle_append_entries(self, data):
        """Handle an AppendEntries RPC, details:

        - if the message is from a leader with equal (or greater) term number, step down:
            (become follower)
        """

        # validate term
        if not self.validate_term(data):
            return

        if self.storage.term >= data['term']:
            self.state.to_follower()


class Leader(State):
    """Raft server's Leader state:

    - when an election is won, a candidate will transition to a leader
    - when transitioning to leader state, send empty messages of type append_entries
        (AppendEntries RPCs) to all other nodes
    - send heartbeats every 'heartbeat_timeout' to prevent election timeouts in other nodes
    - if a command is received, write to local log, replicate, then
        return after the is commited to the local state machine
    - if a follower's last_log_index >= next_index:
        - send message of type append_entries (AppendEntries RPC)
        - the log entries sent in the RPC should start at next_index
        - if the message was successful: update follower's next_index and match_index
        - if the messsage fails:
            - because of inconsistent logs: decrement next_index and retry message
            - because of inconsistent term:
                the local (node's) term < remode (node's) term, become follower
    
    - if there exists an N such that N > commit_index, a majority of match_index[i] ≥ N,
    and log[N].term == self term: set commit_index = N
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.heartbeat_timer = EventTimer(self.generate_heartbeat_timeout, self.heartbeat)

        # we could have a timeout for leaders to step-down when a number of heartbeats
        # aren't received by the mayority, but for now, we just allow for things to
        # function normally, since writes won't work, but reads should still work.
        #
        # Here is the stepdown_timeout timer:
        # self.stepdown_timer = EventTimer(self.generate_stepdown_timeout, self.to_follower())

        # initialize request id
        self.request_id = generate_request_id()
        self.response_map = {}

    def start(self):
        self.init_log()
        self.heartbeat()
        self.heartbeat_timer.start()

        # see __init__() for more info
        # self.stepdown_timer.start()

    def stop(self):
        self.heartbeat_timer.stop()

        # see __init__() for more info
        # self.stepdown_timer.stop()

    def init_log(self):
        self.log.next_index = {
            follower: self.log.last_log_index + 1 for follower in self.raft.members
        }

        self.log.match_index = {
            follower: 0 for follower in self.raft.members
        }

    async def append_entries(self, destination=None):
        """Handle an AppendEntries RPC to replicate log entries and handle heartbeats:

        args:
            destination:
                - (host, udp_port, tcp_port) if sending to a specific member/node
                - None otherwise (default), will broadcast RPC

        Notes:
            - if the local (node's) next_index of the remote (node) 
        """
        destinations = [destination] if destination else [m for k, m in self.raft.members.items()]
        logger.debug(f'sending append_entries RPC to {len(destinations)} members')

        for d in destinations:
            d_id = f'{d[0]}:{d[1]}:{d[2]}'
            logger.debug(f'sending to member: {d_id}')
            data = {
                'type': 'append_entries',
                'term': self.storage.term,
                'leader_id': self.id,
                'commit_index': self.log.commit_index,
                'request_id': self.request_id
            }

            next_index = self.log.next_index[f'{d_id}']
            prev_index = next_index - 1

            if self.log.last_log_index >= next_index:
                data['entries'] = [self.log[next_index]]

            else:
                data['entries'] = []

            data.update({
                'prev_log_index': prev_index,
                'prev_log_term': self.log[prev_index]['term'] if self.log and prev_index else 0
            })

            logger.info(f'Sending to {d_id}, data: {data}')
            asyncio.ensure_future(
                self.raft.send(data=data, dest_host=d[0], dest_port=d[1]), loop=self.loop
            )

    def on_receive_append_entries_response(self, data):

        # apply to state machine and validate term
        self.commit_state_to_commit_index()
        if not self.validate_term(data):
            return

        sender_id = self.state.get_sender_id(data['sender'])

        # Count all unqiue responses per particular heartbeat interval
        # and step down via <step_down_timer> if leader doesn't get majority of responses for
        # <step_down_missed_heartbeats> heartbeats

        if data['request_id'] in self.response_map:
            self.response_map[data['request_id']].add(sender_id)

            if self.state.is_majority(len(self.response_map[data['request_id']]) + 1):
                self.step_down_timer.reset()
                del self.response_map[data['request_id']]

        if not data['success']:
            self.log.next_index[sender_id] = max(self.log.next_index[sender_id] - 1, 1)

        else:
            self.log.next_index[sender_id] = data['last_log_index'] + 1
            self.log.match_index[sender_id] = data['last_log_index']

            self.update_commit_index()

        # Send AppendEntries RPC to continue updating fast-forward log (data['success'] == False)
        # or in case there are new entries to sync (data['success'] == data['updated'] == True)
        if self.log.last_log_index >= self.log.next_index[sender_id]:
            asyncio.ensure_future(self.append_entries(destination=sender_id), loop=self.loop)

    def update_commit_index(self):
        commited_on_majority = 0
        for index in range(self.log.commit_index + 1, self.log.last_log_index + 1):
            commited_count = len([
                1 for follower in self.log.match_index
                if self.log.match_index[follower] >= index
            ])

            # If index is matched on at least half + self for current term — commit
            # That may cause commit fails upon restart with stale logs
            is_current_term = self.log[index]['term'] == self.storage.term
            if self.state.is_majority(commited_count + 1) and is_current_term:
                commited_on_majority = index

            else:
                break

        if commited_on_majority > self.log.commit_index:
            self.log.commit_index = commited_on_majority

    async def execute_command(self, command):
        """Write to log & send AppendEntries RPC"""
        self.apply_future = asyncio.Future(loop=self.loop)

        entry = self.log.write(self.storage.term, command)
        asyncio.ensure_future(self.append_entries(), loop=self.loop)

        await self.apply_future

    def heartbeat(self):
        self.request_id += 1
        self.response_map[self.request_id] = set()
        asyncio.ensure_future(self.append_entries(), loop=self.loop)


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
        self.state_machine = PersistentStateMachine()
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