import os
import json
import logging
import msgpack
import collections

logger = logging.getLogger(__name__)


class PersistentLog(collections.UserList):
    """Persistent Raft's replicated log on disk
    Log entry structure:
        {term: <term>, command: <command>}

    Then entry's index is its corresponding line number
    """

    # update the cache every <counter> entries
    UPDATE_CACHE_COUNTER = 25
    SERIALIZER = False

    def __init__(self, node_id, log_path=None, reset_log=False):

        self.cache = list()
        self.__init_log_and_cache(node_id, log_path=log_path, reset_log=reset_log)

        # Volatile states (lost after restart)

        # ####################
        # #### ANY state #####
        # ####################

        # index of highest log entry known to be committed
        # (initialized to 0, increases monotonically)"""
        self.commit_index = 0

        """Volatile state on all servers: index of highest log entry applied to state machine
        (initialized to 0, increases monotonically)"""
        self.last_applied = 0

        # ####################
        # ##### Leaders ######
        # ####################

        # Volatile state on Leaders: for each server,
        # index of the next log entry to send to that server
        #
        # Initialize to leader's last log index + 1
        # {<follower_id>:  index, ...}
        self.next_index = {}

        """Volatile state on Leaders: for each server,
        index of highest log entry known to bereplicated on server
        (initialized to 0, increases monotonically)
            {<follower>:  index, ...}
        """
        self.match_index = {}

    def __init_log_and_cache(self, node_id, log_path=None, reset_log=False):
        logger.debug('Initializing log')

        # filename should be "<log_path>/ip_port.log"
        self.filename = os.path.join(log_path, f'{node_id.replace(":", "_")}.log'.format())

        # create log's directory structure if it doesn't exist
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)

        # open log in 'append' mode
        open(self.filename, 'a').close()

        if reset_log and os.path.isfile(self.filename):
            # remove old log
            os.remove(self.filename)
            # touch/create new empty log
            open(self.filename, 'a').close()
            logger.debug('Reseting persistent log.')
        elif os.path.isfile(self.filename):
            logger.debug('Using existing persistent log')

        self.cache = self.read()

    def __getitem__(self, index):
        return self.cache[index - 1]

    def __bool__(self):
        return bool(self.cache)

    def __len__(self):
        return len(self.cache)

    def __pack(self, data):
        if self.SERIALIZER:
            return msgpack.packb(data, use_bin_type=True)
        else:
            return json.dumps(data).encode()

    def __unpack(self, data):
        if self.SERIALIZER:
            # TODO: find out how to unpack multi-line messages
            return msgpack.unpackb(data, use_list=True, encoding='utf-8')
        else:
            decoded = data.decode() if isinstance(data, bytes) else data
            return json.loads(decoded)

    def write(self, term, command):
        with open(self.filename, 'ab') as log_file:
            entry = {
                'term': term,
                'command': command
            }
            log_file.write(self.__pack(entry) + '\n'.encode())

        self.cache.append(entry)
        if not len(self) % self.UPDATE_CACHE_COUNTER:
            self.cache = self.read()

        return entry

    def read(self):
        with open(self.filename, 'rb') as f:
            return [self.__unpack(entry) for entry in f.readlines()]

    def erase_from(self, index):
        updated = self.cache[:index - 1]
        open(self.filename, 'wb').close()
        self.cache = []

        for entry in updated:
            self.write(entry['term'], entry['command'])

    @property
    def last_log_index(self):
        """Index of last log entry staring from _one_"""
        return len(self.cache)

    @property
    def last_log_term(self):
        if self.cache:
            return self.cache[-1]['term']

        return 0
