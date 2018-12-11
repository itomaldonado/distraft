import asyncio
import functools
import logging
import random

from timers import EventTimer
from enum import Enum

logger = logging.getLogger(__name__)

HEARTBEAT_TIMEOUT_LOWER = 0.150
ELECTION_TIMEOUT_LOWER = 0.200
TIMEOUT_SPREAD_RATIO = 2.0


class States(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 3


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


def validate_term(func):
    """Compares current term and request term:
        if current term is stale:
            update current term & become follower
        if received term is stale:
            respond with False
    """

    @functools.wraps(func)
    def handle_message_function(self, data):
        if self.storage.term < data['term']:
            self.storage.update({'term': data['term']})
            if not isinstance(self, Follower):
                self.raft.to_state(States.FOLLOWER)

        if self.storage.term > data['term'] and not data['type'].endswith('_response'):
            response = {
                'success': False,
                'term': self.storage.term,
                'type': f'{data["type"]}_response',
            }
            asyncio.ensure_future(self.raft.send(response, data['sender']), loop=self.loop)
            return

        return func(self, data)
    return handle_message_function


class State:
    def __init__(self, raft=None):
        """This is the base state class that all other states must implement"""
        self.raft = raft
        self.storage = self.raft.state_storage
        self.log = self.raft.log
        self.state_machine = self.raft.state_machine
        self.id = self.raft.id
        self.loop = self.raft.loop

    @validate_term
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

    @validate_term
    def handle_message_request_vote_response(self, data):
        """RequestVote RPC response — see handle_message_request_vote implementation details

        message example:
        {
            'type': 'request_vote_response',
            'term': 5,
            'vote_granted': True
        }
        """

    @validate_term
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

    @validate_term
    def handle_message_entries_response(self, data):
        """AppendEntries RPC response — see handle_message_append_entries implementation details

        message example:
        {
            'type': 'append_entries_response',
            'term': 5,
            'success': True
        }
        """


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

    @staticmethod
    def generate_election_timeout():
        to = random.uniform(
            float(HEARTBEAT_TIMEOUT_LOWER),
            float(TIMEOUT_SPREAD_RATIO*HEARTBEAT_TIMEOUT_LOWER)
        )
        logger.debug(f'Generating timeout of: {to} seconds.')
        return to

    @validate_commit_index
    @validate_term
    def handle_append_entries(self, data):
        
        self.raft.set_leader(data['leader_id'])

        # Reply False if log doesn’t contain an entry at prev_log_index whose term matches prev_log_term
        try:
            prev_log_index = data['prev_log_index']
            if prev_log_index > self.log.last_log_index or (
                prev_log_index and self.log[prev_log_index]['term'] != data['prev_log_term']
            ):
                response = {
                    'type': 'append_entries_response',
                    'term': self.storage.term,
                    'success': False,

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
                self.log.erase_from(new_index)
        except IndexError:
            pass

        # It's always one entry for now
        for entry in data['entries']:
            self.log.write(entry['term'], entry['command'])

        # Update commit index if necessary
        if self.log.commit_index < data['commit_index']:
            self.log.commit_index = min(data['commit_index'], self.log.last_log_index)

        # Respond True since entry matching prev_log_index and prev_log_term was found
        response = {
            'type': 'append_entries_response',
            'term': self.storage.term,
            'success': True,

            'last_log_index': self.log.last_log_index,
            'request_id': data['request_id']
        }
        asyncio.ensure_future(self.state.send(response, data['sender']), loop=self.loop)

        self.election_timer.reset()

    @validate_term
    def handle_request_vote(self, data):
        """Handle a RequestVote RPC, details:

        The candidate's log has to **always** be up-to-date (equal or better data).

        Here are some rules for comparing local vs candidate logs:
            - last entries with different terms, then the log with the later term is more up-to-date
            - logs that end with the same term, then whichever log is longer is more up-to-date
        """

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

    def to_candidate(self):
        self.raft.to_state(States.CANDIDATE)


class Candidate(State):
    pass


class Leader(State):
    pass
