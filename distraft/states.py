import asyncio
import functools
import logging

logger = logging.getLogger(__name__)


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
    def on_receive_function(self, data):
        if self.storage.term < data['term']:
            self.storage.update({
                'term': data['term']
            })
            if not isinstance(self, Follower):
                self.state.to_follower()

        if self.storage.term > data['term'] and not data['type'].endswith('_response'):
            response = {
                'type': '{}_response'.format(data['type']),
                'term': self.storage.term,
                'success': False
            }
            asyncio.ensure_future(self.state.send(response, data['sender']), loop=self.loop)
            return

        return func(self, data)
    return on_receive_function


class BaseState:
    def __init__(self, state):
        self.state = state

        self.storage = self.state.storage
        self.log = self.state.log
        self.state_machine = self.state.state_machine

        self.id = self.state.id
        self.loop = self.state.loop

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


class Follower(BaseState):
    pass


class Candidate(BaseState):
    pass


class Leader(BaseState):
    pass
