import asyncio
import functools
import logging
import os
import random
import socket

from config import config
from enum import Enum
from networking import PeerProtocol
from storage import PersistentLog, PersistentDict, PersistentStateMachine
from timers import EventTimer
from uuid import uuid1

logger = logging.getLogger(__name__)


def generate_request_id():
    """Generate a new UUID request id
    returns:
        string uuid
    """
    return str(uuid1())


class States(Enum):
    """Enumerate the states a node can take"""
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 3


def validate_term(fun):
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
    @functools.wraps(fun)
    def wrapped(self, data):
        logger.debug(f'{self.id} validate_term() start.')
        if self.storage['term'] < data['term']:
            self.storage.update({'term': data['term']})
            if not isinstance(self, Follower):
                self.to_follower()
                logger.debug(f'{self.id} validate_term() done, bad term, moved to Follower.')
                return False

        if self.storage['term'] > data['term'] and not data['type'].endswith('_response'):
            response = {
                'success': False,
                'term': self.storage['term'],
                'type': f'{data["type"]}_response',
            }
            sender = self.raft.members.get(data['sender_id'])
            host = sender[0]
            port = sender[1]
            asyncio.ensure_future(
                self.raft.send(data=response, dest_host=host, dest_port=port), loop=self.loop
            )
            logger.debug(f'{self.id} validate_term() done, bad term, responded with False.')
            return False

        logger.debug(f'{self.id} validate_term() done, good term.')
        return fun(self, data)
    return wrapped


def validate_commit_index(func):
    """Apply to State Machine everything up to commit index"""

    @functools.wraps(func)
    def wrapped(self, *args, **kwargs):
        logger.debug(f'{self.id} validate_commit_index() start.')
        for not_applied in range(self.log.last_applied + 1, self.log.commit_index + 1):
            self.state_machine.commit(self.log[not_applied]['command'])
            self.log.last_applied += 1

            try:
                self.apply_future.set_result(not_applied)
            except (asyncio.futures.InvalidStateError, AttributeError):
                pass
        logger.debug(f'{self.id} validate_commit_index() done.')
        return func(self, *args, **kwargs)
    return wrapped


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
    def handle_request_vote(self, data):
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
            'candidate_id': 'server0',
            'sender_id': 'server0',
            'last_log_index': 10,
            'last_log_term': 4
        }
        """

    # remember to validate the term
    def handle_request_vote_response(self, data):
        """RequestVote RPC response — see handle_request_vote implementation details

        message example:
        {
            'type': 'request_vote_response',
            'term': 5,
            'vote_granted': True,
            'sender_id': 'server0'
        }
        """

    # remember to validate the term
    def handle_append_entries(self, data):
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
            'leader_id': 'server0',
            'sender_id': 'server0',
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
    def handle_append_entries_response(self, data):
        """AppendEntries RPC response — see handle_append_entries implementation details

        message example:
        {
            'type': 'append_entries_response',
            'term': 5,
            'success': True,
            'sender_id': 'server0'
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

    @property
    def name(self):
        return "Base"

    @staticmethod
    def generate_election_timeout():
        to = random.uniform(
            float(config.ELECTION_TIMEOUT),
            float(config.TIMEOUT_SPREAD_RATIO*config.ELECTION_TIMEOUT)
        )
        return to

    @staticmethod
    def generate_heartbeat_timeout():
        to = random.uniform(
            float(config.HEARTBEAT_TIMEOUT),
            float(config.TIMEOUT_SPREAD_RATIO*config.HEARTBEAT_TIMEOUT)
        )
        return to

    @staticmethod
    def generate_stepdown_timeout():
        lower = config.HEARTBEAT_TIMEOUT*config.MISSED_HEARTBEATS
        to = random.uniform(
            float(lower),
            float(config.TIMEOUT_SPREAD_RATIO*lower)
        )
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
        logger.info(f'{self.id} became a Folllower.')
        # (always) set voted_for to None
        self.storage.update({'voted_for': None})

        # (if unexistent) set term to zero"""
        if not self.storage.get('term', None):
            logger.debug(f'{self.id} term not found in storage, reseting to 0.')
            self.storage.update({'term': 0})

        # start election timer
        self.election_timer.start()

    def stop(self):
        # stop the election timer
        self.election_timer.stop()

    @validate_commit_index
    @validate_term
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
        logger.debug(f'{self.id} handle_append_entries(), data: {data}')

        success, response = self._check_and_append_log_entriex(data)

        if success:
            logger.debug(
                f'{self.id} appended entries from {data["leader_id"]} '
                f'at: {response["last_log_index"]}'
            )
        else:
            logger.warning(
                f'{self.id} did not append data from {data["leader_id"]} from '
                f'leader\'s {data["prev_log_index"]}, entries: {data["entries"]}'
            )

        # send response to sender
        sender = self.raft.members.get(data['sender_id'])
        host = sender[0]
        port = sender[1]
        asyncio.ensure_future(
            self.raft.send(data=response, dest_host=host, dest_port=port), loop=self.loop
        )

        # since we got a message, reset the election timer
        self.election_timer.reset()

    def _check_and_append_log_entriex(self, data):
        """Validates log entries and appends if necesary
        args:
            - data 'append_entries' message received

        returns:
            - True if check/append was sucessful (False otherwise)
            - respnse dictionary to send as response
        """
        # Reply False if log doesn’t contain an entry at prev_log_index
        # whose term matches prev_log_term
        try:
            # checks for success (assume false first)
            prev_log_index = data['prev_log_index']

            # if validation failed, return False
            logger.debug(
                f'{self.id} check indexes/terms: ' +
                f'{prev_log_index} > {self.log.last_log_index} or ' +
                f'{self.log[prev_log_index]["term"]} != {data["prev_log_term"]}'
            )
            if ((prev_log_index > self.log.last_log_index) or
               (self.log[prev_log_index]['term'] != data['prev_log_term'])):
                response = {
                    'type': 'append_entries_response',
                    'term': self.storage['term'],
                    'success': False,
                    'request_id': data['request_id'],
                    'sender_id': self.id
                }
                return False, response

        except IndexError:
            logger.debug(
                f'{self.id} check went out of bounds, possibly because it is the first append'
            )
            pass

        # if term is valid and append_entries comes, set leader to the ID
        logger.debug(f'{self.id} setting leader to {data["leader_id"]}')
        self.raft.set_leader(data['leader_id'])

        # If the new entry happens to be a duplicate (same index same term),
        # we don't do anything and respond with 'SUCCESS'
        new_index = data['prev_log_index'] + 1
        try:
            logger.debug(
                f'{self.id} check if we need to do nothing: ' +
                f'{self.log[new_index]["term"]} == {data["term"]}'
            )
            if self.log[new_index]['term'] == data['term']:
                # Respond True since there was an entry matching already matching
                # prev_log_index and prev_log_term found
                response = {
                    'type': 'append_entries_response',
                    'term': self.storage['term'],
                    'success': True,
                    'last_log_index': self.log.last_log_index,
                    'request_id': data['request_id'],
                    'sender_id': self.id
                }
                return True, response
        except IndexError:
            logger.debug(
                f'{self.id} check went out of bounds, possibly because it is the first append'
            )
            pass

        # If an existing entry conflicts with a new one (same index but different terms),
        # delete the existing entry and all that follow it
        new_index = data['prev_log_index'] + 1
        try:
            logger.debug(
                f'{self.id} check if we need to delete: ' +
                f'{self.log[new_index]["term"]} != {data["term"]} or ' +
                f'{self.log.last_log_index} != {prev_log_index}'
            )
            if self.log[new_index]['term'] != data['term'] or (
                self.log.last_log_index != prev_log_index
            ):
                self.log.delete_from(new_index)
        except IndexError:
            logger.debug(
                f'{self.id} check went out of bounds, possibly because it is the first append'
            )
            pass

        # write all entries received in the message to local (node's) log, in order
        logger.debug(f'{self.id} appending entries from "append_entries" message')
        for entry in data['entries']:
            self.log.write(entry['term'], entry['command'])

        # Update commit index if/when necessary
        if self.log.commit_index < data['commit_index']:
            self.log.commit_index = min(data['commit_index'], self.log.last_log_index)

        # we appended entries, return True
        # Respond True since there was an entry matching prev_log_index and prev_log_term found
        response = {
            'type': 'append_entries_response',
            'term': self.storage['term'],
            'success': True,
            'last_log_index': self.log.last_log_index,
            'request_id': data['request_id'],
            'sender_id': self.id
        }
        return True, response

    @validate_term
    def handle_request_vote(self, data):
        """Handle a RequestVote RPC, details:

        The candidate's log has to **always** be up-to-date (equal or better data).

        Here are some rules for comparing local vs candidate logs:
            - last entries with different terms, then the log with the later term is more up-to-date
            - logs that end with the same term, then whichever log is longer is more up-to-date
        """
        logger.debug(f'{self.id} handle_request_vote(), data: {data}')

        # If you have voted for this term, don't vote again~
        if self.storage.get('voted_for', None) is None:

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
                'term': self.storage['term'],
                'vote_granted': vote,
                'sender_id': self.id
            }

            # if going to vote for the candidate, udpate local storage
            if vote:
                self.storage.update({'voted_for': data['candidate_id']})

            # send response to sender
            sender = self.raft.members.get(data['sender_id'])
            host = sender[0]
            port = sender[1]
            asyncio.ensure_future(
                self.raft.send(data=response, dest_host=host, dest_port=port), loop=self.loop
            )

        # since we got a message, reset the election timer
        self.election_timer.reset()

    @property
    def name(self):
        return "Follower"


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
        self.election_timer = EventTimer(self.generate_election_timeout, self.election_timedout)
        self.voting_timer = EventTimer(self.generate_heartbeat_timeout, self.request_vote)
        self.vote_count = 0

    def election_timedout(self):
        logger.info(f'{self.id} Candidate election timed out, becoming follower.')
        self.to_follower()

    def start(self):
        """Increment current term, vote for herself & send vote requests"""
        logger.info(f'{self.id} became a candidate for term {self.storage["term"] + 1}.')
        self.storage.update({
            'term': self.storage['term'] + 1,
            'voted_for': self.id
        })

        self.vote_count += 1
        self.voting_timer.start()
        self.election_timer.start()

    def stop(self):
        self.voting_timer.stop()
        self.election_timer.stop()

    def request_vote(self):
        """Send message of type request_vote (RequestVote RPC): gather votes form nodes"""
        logger.info(f'{self.id} Candidate requesting votes from members.')
        data = {
            'type': 'request_vote',
            'term': self.storage['term'],
            'candidate_id': self.id,
            'sender_id': self.id,
            'last_log_index': self.log.last_log_index,
            'last_log_term': self.log.last_log_term
        }
        self.raft.broadcast(data)

    @validate_term
    def handle_request_vote_response(self, data):
        """Handle responses from a RequestVote RPC, details:

        - if the remote node voted for this (local) node/candidate, add to count.
        - check for majority, if we got it, transition to leader
        """
        logger.debug(f'{self.id} handle_request_vote_response(), data: {data}')

        if data.get('vote_granted'):
            self.vote_count += 1

            if self.raft.is_majority(self.vote_count):
                logger.info(
                    f'{self.id} Candidate has enough votes '
                    f'({self.vote_count}), changing state to leader'
                )
                self.to_leader()
            else:
                logger.info(
                    f'{self.id} Candidate does not have enough votes yet, votes: {self.vote_count}'
                )

    @validate_term
    def handle_request_vote(self, data):
        """Handle a RequestVote RPC, details:

        The candidate's log has to **always** be up-to-date (equal or better data).

        Here are some rules for comparing local vs candidate logs:
            - last entries with different terms, then the log with the later term is more up-to-date
            - logs that end with the same term, then whichever log is longer is more up-to-date
        """
        logger.debug(f'{self.id} handle_request_vote(), data: {data}')
        pass

    @validate_term
    def handle_append_entries(self, data):
        """Handle an AppendEntries RPC, details:

        - if the message is from a leader with equal (or greater) term number, step down:
            (become follower)
        """
        logger.debug(f'{self.id} handle_append_entries(), data: {data}')

        if self.storage['term'] >= data['term']:
            self.to_follower()

    @property
    def name(self):
        return "Candidate"


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
    - if there exists an N such that N > commit_index, a majority of match_index[i] >= N,
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
        logger.info(f'{self.id} became a Leader for term {self.storage["term"]}.')
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
        self.log.next_index = {}
        for follower in self.raft.members:
            if follower != self.id:
                self.log.next_index.update({follower: self.log.last_log_index + 1})

        self.log.match_index = {}
        for follower in self.raft.members:
            if follower != self.id:
                self.log.match_index.update({follower: 0})

    async def append_entries(self, destination_id=None, reset_timer=False):
        """Handle an AppendEntries RPC to replicate log entries and handle heartbeats:

        args:
            destination:
                - destination_id if sending to a specific member/node
                - None otherwise (default), will broadcast RPC

        Notes:
            - if the local (node's) next_index of the remote (node)
        """
        if destination_id:
            destinations = [{'id': destination_id, 'address': self.members[destination_id]}]
        else:
            destinations = []
            for k, v in self.raft.members.items():
                if k != self.id:
                    destinations.append({'id': k, 'address': v})
        logger.debug(f'{self.id} sending append_entries RPC to {len(destinations)} members')

        for dest in destinations:
            d_id = dest['id']
            d = dest['address']
            logger.debug(f'{self.id} sending to member: {d_id}')
            data = {
                'type': 'append_entries',
                'term': self.storage['term'],
                'leader_id': self.id,
                'sender_id': self.id,
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

            logger.debug(f'{self.id} Sending message to {d_id}, data: {data}')
            asyncio.ensure_future(
                self.raft.send(data=data, dest_host=d[0], dest_port=d[1]), loop=self.loop
            )
        if reset_timer:
            self.heartbeat_timer.reset()

    @validate_commit_index
    @validate_term
    def handle_append_entries_response(self, data):
        logger.debug(f'{self.id} handle_append_entries_response(), data: {data}')

        sender_id = data['sender_id']

        # Count all unqiue responses per particular heartbeat interval
        # and step down via <step_down_timer> if leader doesn't get majority of responses for
        # <step_down_missed_heartbeats> heartbeats

        if data['request_id'] in self.response_map:
            self.response_map[data['request_id']].add(sender_id)

            if self.raft.is_majority(len(self.response_map[data['request_id']]) + 1):
                # self.step_down_timer.reset()
                del self.response_map[data['request_id']]

        if data['success']:
            self.log.next_index[sender_id] = data['last_log_index'] + 1
            self.log.match_index[sender_id] = data['last_log_index']
            self.update_commit_index()
            try:
                self.raft.apply_future.set_result(self.log.last_applied)
            except (asyncio.futures.InvalidStateError, AttributeError):
                pass
        else:
            self.log.next_index[sender_id] = max(self.log.next_index[sender_id] - 1, 1)

        # Send AppendEntries RPC to continue updating fast-forward log (data['success'] == False)
        # or in case there are new entries to sync (data['success'] == data['updated'] == True)
        if self.log.last_log_index >= self.log.next_index[sender_id]:
            asyncio.ensure_future(self.append_entries(destination=sender_id), loop=self.loop)

    @validate_term
    def handle_append_entries(self, data):
        """Handle an AppendEntries RPC, details:

        - if the message is from a leader with equal (or greater) term number, step down:
            (become follower)
        """
        logger.debug(f'{self.id} handle_append_entries(), data: {data}')
        pass

    def update_commit_index(self):
        commited_on_majority = 0
        for index in range(self.log.commit_index + 1, self.log.last_log_index + 1):
            commited_count = len([
                1 for follower in self.log.match_index
                if self.log.match_index[follower] >= index
            ])

            # If index is matched on at least half + self for current term — commit
            # That may cause commit fails upon restart with stale logs
            is_current_term = self.log[index]['term'] == self.storage['term']
            if self.raft.is_majority(commited_count + 1) and is_current_term:
                commited_on_majority = index

            else:
                break

        if commited_on_majority > self.log.commit_index:
            self.log.commit_index = commited_on_majority

    def heartbeat(self):
        self.request_id = generate_request_id()
        self.response_map[self.request_id] = set()
        asyncio.ensure_future(self.append_entries(), loop=self.loop)

    @property
    def name(self):
        return "Leader"


class Raft:

    def __init__(
            self,
            node_id=None,
            members=None,
            host=None, udp_port=None,
            client_host=None, tcp_port=None,
            loop=None):
        """Creates a Raft consensus server to be used however needed

        args:
            - node_id: the id of this node
            - host: hostname / IP for this server
            - host: port number for this server
            - loop: asyncio loop object
            - members: dictionary of members, like:
                {
                    'server0': ('127.0.0.1', 9000, '127.0.0.1', 5000),
                    'server1': ('127.0.0.1', 9001, '127.0.0.1', 5001),
                    'server2': ('127.0.0.1', 9002, '127.0.0.1', 5002),
                }
            - members list **includes** local node
        """
        self.id = node_id
        self.host = host
        self.udp_port = udp_port
        self.client_host = client_host
        self.tcp_port = tcp_port
        self.loop = loop if loop else asyncio.get_event_loop()
        self.queue = asyncio.Queue(loop=self.loop)
        self._leader = None
        self.apply_future = None
        self.state = None
        self.up = False

        # cluster list
        self.members = members if members else {}
        logger.debug(f'{self.id} has these members: {self.members}')

        # Initialize logs and persitent data
        self.state_machine = PersistentStateMachine(
            node_id=self.id, path=config.DATA_PATH,
            reset=config.RESET_LOGS
        )
        self.state_storage = PersistentDict(
            path=os.path.join(config.DATA_PATH, f'{self.id}.state'),
            reset=config.RESET_LOGS
        )
        self.log = PersistentLog(node_id=self.id, log_path=config.LOG_PATH, reset=config.RESET_LOGS)

    def network_request_handler(self, data):
        logger.debug(f'{self.id} received message of type: {data["type"]} from {data["sender"]}')
        try:
            handler = getattr(self.state, f'handle_{data["type"]}')
        except AttributeError:
            logger.warning(
                f'{self.id} state "{self.state.name}" cannot handle '
                f'message of type: {data["type"]}.'
            )
            return
        handler(data)

    async def start(self):
        logger.info(
            f'{self.id} starting raft server with info: {self.host}:{self.udp_port}:{self.tcp_port}'
        )
        protocol = PeerProtocol(
            network_queue=self.queue,
            network_request_handler=self.network_request_handler,
            loop=self.loop
        )
        self.transport, _ = await asyncio.Task(
            self.loop.create_datagram_endpoint(protocol, local_addr=(self.host, self.udp_port,)),
            loop=self.loop
        )

        # Raft server **always** starts up as a Follower
        self.state = Follower(raft=self)
        self.state.start()
        self.up = True

    def stop(self):
        logger.info(f'{self.id} stopping raft server')
        self.state.stop()
        self.transport.close()
        self.up = False

    async def send(self, data, dest_host, dest_port):
        """Sends data to destination Node
        Args:
            data — serializable object
            destination — tuple (host, port), example: (127.0.0.1, 8000)
        """
        dest_host = socket.gethostbyname(dest_host)
        destination = (dest_host, dest_port,)
        logger.debug(f'{self.id} sending message to {destination}, data: {data}.')
        await self.queue.put({'data': data, 'destination': destination})

    def broadcast(self, data):
        """Sends data to all Members in cluster

        Note: self.members list does not include the local node's info
        """
        for k, m in self.members.items():
            # send messages to everyone **but** yourself
            if k != self.id:
                asyncio.ensure_future(
                    self.send(data=data, dest_host=m[0], dest_port=m[1]),
                    loop=self.loop
                )

    @property
    def leader(self):
        return self._leader

    @property
    def leader_address(self):
        if self.leader:
            return self.members.get(self.leader, None)
        else:
            return None

    @property
    def leader_client_address(self):
        full_tuple = self.leader_address
        if full_tuple:
            return f'{full_tuple[2]}:{full_tuple[3]}'
        else:
            return None

    def set_leader(self, leader_id):
        self._leader = leader_id

    def to_state(self, state):
        if state == States.FOLLOWER:
            self._change_state(Follower)
            self.set_leader(None)
        elif state == States.CANDIDATE:
            self._change_state(Candidate)
            self.set_leader(None)
        elif state == States.LEADER:
            self._change_state(Leader)
            self.set_leader(self.id)
        else:  # do nothing
            pass

    def _change_state(self, new_state):
        self.state.stop()
        self.state = new_state(raft=self)
        self.state.start()

    def is_majority(self, count):
        return count > (len(self.members.values()) // 2)

    def __write_log(self, term, command={}):
        logger.info(f'{self.id} log: writing --> term:{term}, command: {command}')
        self.log.write(term, command)

    async def execute_command(self, command):
        """Write to log & send AppendEntries RPC"""
        self.apply_future = asyncio.Future(loop=self.loop)
        self.__write_log(self.state_storage['term'], command)
        asyncio.ensure_future(self.state.append_entries(reset_timer=True), loop=self.loop)
        await self.apply_future

    async def set_value(self, key, value):
        if (self.leader) and (self.leader == self.id):
            await self.execute_command({key: value})

    async def get_value(self, key):
        return self.state_machine.get(key, None)
