import os
import json
import logging
import msgpack
import collections

from config import config

logger = logging.getLogger(__name__)


class PersistentDict(collections.UserDict):
    """A Dictionary data structure that is automagically persisted to disk as json."""
    def __init__(self, data=None, path=None, reset=False):
        # set persistent dict's path variable
        self.filename = path

        # create file's directory structure if it doesn't exist
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)

        if reset and os.path.isfile(self.filename):
            # remove old log
            os.remove(self.filename)
            # touch/create new empty dict
            with open(self.filename, 'a') as f:
                f.writelines(['{}'])
            logger.debug('Reseting persistent dictionary.')
        elif not os.path.isfile(self.filename):
            # touch/create new empty dict
            with open(self.filename, 'a') as f:
                f.writelines(['{}'])
        elif os.path.isfile(self.filename):
            logger.debug('Using existing persistent dictionary.')

        # load the data from file
        data = data if data else {}
        if os.path.isfile(self.filename):
            with open(self.filename, 'r') as f:
                data = json.loads(f.read())

        super().__init__(data)

    def __setitem__(self, key, value):
        self.data[self.__keytransform__(key)] = value
        self.persist()

    def __delitem__(self, key):
        del self.data[self.__keytransform__(key)]
        self.persist()

    def __keytransform__(self, key):
        return key

    def persist(self):
        with open(self.filename, 'w+') as f:
            f.write(json.dumps(self.data))


class PersistentLog(collections.UserList):
    """Persistent Raft's replicated log on disk
    Log entry structure:
        {term: <term>, command: <command>}

    Then entry's index is its corresponding line number
    """

    def __init__(self, data=None, node_id=None, log_path=None, reset=False):

        logger.debug(f'{node_id} initializing persistent log.')

        # Volatile states (lost after restart)

        # ####################
        # #### ANY state #####
        # ####################

        self.id = node_id

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

        # filename should be "<log_path>/ip_port.log"
        self.filename = os.path.join(log_path, f'{node_id.replace(":", "_")}.log'.format())

        # create file's directory structure if it doesn't exist
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)

        # open log in 'append' mode
        open(self.filename, 'a').close()

        if reset and os.path.isfile(self.filename):
            # remove old log
            os.remove(self.filename)
            # touch/create new empty log
            open(self.filename, 'a').close()
            logger.debug(f'{self.id} reseting persistent log.')
        elif os.path.isfile(self.filename):
            logger.debug(f'{self.id} using existing persistent log.')

        # load the data from file
        data = data if data else {}
        data = self.read()

        # we use a cache instead of the self.data because
        # self.data may be overritten by other methods, so until we
        # implement those methods I'd rather use self.cache
        self.cache = data
        # super().__init__(data)

    def __getitem__(self, index):
        logger.debug(f'{self.id} get persistent log entry at: {index}')
        return_val = self.cache[index - 1]
        logger.debug(f'{self.id} log entry at: {index} is {return_val}')
        return return_val

    def __bool__(self):
        return bool(self.cache)

    def __len__(self):
        return len(self.cache)

    def __pack(self, data):
        if config.SERIALIZER in ['msgpack']:
            return msgpack.packb(data, use_bin_type=True)
        else:
            return json.dumps(data).encode()

    def __unpack(self, data):
        if config.SERIALIZER in ['msgpack']:
            # TODO: find out how to unpack multi-line messages
            return msgpack.unpackb(data, use_list=True, encoding='utf-8')
        else:
            decoded = data.decode() if isinstance(data, bytes) else data
            return json.loads(decoded)

    def write(self, term, command):
        logger.debug(f'{self.id} write persistent log entry "{command}" at: {len(self.cache)}')
        with open(self.filename, 'ab') as log_file:
            entry = {'term': term, 'command': command}
            log_file.write(self.__pack(entry) + '\n'.encode())
        self.cache.append(entry)
        # if not len(self) % UPDATE_CACHE_COUNTER:
        #     self.cache = self.read()

        return entry

    def read(self):
        with open(self.filename, 'rb') as f:
            return [self.__unpack(entry) for entry in f.readlines()]

    def delete_from(self, index):
        logger.debug(f'{self.id} delete persistent log entries starting at: {index}.')
        updated = self.cache[:index - 1]
        open(self.filename, 'wb').close()
        self.cache = []
        for entry in updated:
            self.write(entry['term'], entry['command'])

    @property
    def last_log_index(self):
        """Index of last log entry staring from 1"""
        last_log_index = len(self.cache)
        logger.debug(f'{self.id} last_log_index at: {last_log_index}.')
        return (last_log_index)

    @property
    def last_log_term(self):
        last_log_term = self.cache[-1]['term'] if self.cache else 0
        logger.debug(f'{self.id} last_log_term at: {last_log_term}.')
        return last_log_term


class PersistentStateMachine(PersistentDict):
    """Raft Replicated State Machine — a persistent dictionary"""

    def __init__(self, node_id=None, path=None, reset=False):
        self.id = node_id
        self.data_filename = os.path.join(path, f'{self.id.replace(":", "_")}.data')
        super().__init__(path=self.data_filename, reset=reset)

    def commit(self, command):
        """Commit a command to State Machine"""
        logger.debug(f'{self.id} commit command: {command}.')
        self.update(command)
