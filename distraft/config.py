import os
import logging


def parse_bool(string):
    return True if string.strip().lower() in ['true', '1', 't', 'y', 'yes'] else False


def get_log_level(string):
    levels = {
        'error': logging.ERROR,
        'warning': logging.WARNING,
        'info': logging.INFO,
        'debug': logging.DEBUG,
    }
    return levels.get(string.strip().lower(), logging.INFO)


def get_serializer(string):
    return 'msgpack' if string.strip().lower() in ['msgpack'] else 'json'


class Configuration:
    def __init__(self):
        self.configure()

    def configure(self):

        # path where the persistent log will be written to
        setattr(
            self,
            'LOG_PATH',
            os.path.abspath(os.environ.get('DISTRAFT_LOG_PATH', '/etc/distraft/'))
        )

        # path where the state machine and local state store will be written to
        setattr(
            self,
            'DATA_PATH',
            os.path.abspath(os.environ.get('DISTRAFT_DATA_PATH', '/etc/distraft/'))
        )

        # the ratio/spread the timeouts will have when randomizing (defaults to double the timeout)
        setattr(
            self,
            'TIMEOUT_SPREAD_RATIO',
            float(os.environ.get('DISTRAFT_TIMEOUT_SPREAD_RATIO', '2.0'))
        )

        # heartbeat timeouts (wait between each heartbeat)
        # should be lower than election timeout
        # this is the lower end, the full interval/spreas is calculated by:
        # `HEARTBEAT_TIMEOUT` * `TIMEOUT_SPREAD_RATIO`
        setattr(
            self,
            'HEARTBEAT_TIMEOUT',
            float(os.environ.get('DISTRAFT_HEARTBEAT_TIMEOUT', '0.100'))
        )

        # How many heartbeats can be un-answered before a leader becomes a follower
        setattr(
            self,
            'MISSED_HEARTBEATS',
            float(os.environ.get('DISTRAFT_MISSED_HEARTBEATS', '5'))
        )

        # election timeouts (wait before becoming a candidate/follower)
        # should be greater than heartbeat timeout
        # this is the lower end, the full interval/spreas is calculated by:
        # `ELECTION_TIMEOUT_LOWER` * `TIMEOUT_SPREAD_RATIO`
        setattr(
            self,
            'ELECTION_TIMEOUT',
            float(os.environ.get('DISTRAFT_ELECTION_TIMEOUT', '0.500'))
        )

        # reset logs, state data and state machine on start-up
        setattr(
            self,
            'RESET_LOGS',
            parse_bool(os.environ.get('DISTRAFT_RESET_LOGS', 'False'))
        )

        # maximun number of items (append_entries RPCs) that leaders/followers will maintain
        setattr(
            self,
            'MAX_REQUEST_CACHE_LENGTH',
            int(os.environ.get('DISTRAFT_MAX_REQUEST_CACHE_LENGTH', '1000'))
        )

        # return nicely-formatted json on http responses
        setattr(
            self,
            'PRETTY_PRINT_RESPONSES',
            parse_bool(os.environ.get('DISTRAFT_PRETTY_PRINT_RESPONSES', 'False'))
        )

        # web server debug mode
        setattr(
            self,
            'WEB_SERVER_DEBUG',
            parse_bool(os.environ.get('DISTRAFT_WEB_SERVER_DEBUG', 'False'))
        )

        # log-level (logs are printed to stderr/stdout)
        setattr(
            self,
            'LOG_LEVEL',
            get_log_level(os.environ.get('DISTRAFT_LOG_LEVEL', 'INFO'))
        )

        # network and data serializer (json or msgpack *only*)
        setattr(
            self,
            'SERIALIZER',
            get_serializer(os.environ.get('DISTRAFT_SERIALIZER', 'json'))
        )

        # The name of this node, used for static cluster bootstraping
        # each node must have a unique name
        setattr(
            self,
            'NAME',
            os.environ.get('DISTRAFT_NAME', 'default').strip().lower()
        )

        setattr(
            self,
            'HOST',
            os.environ.get('DISTRAFT_HOST', '127.0.0.1').strip().lower()
        )
        setattr(
            self,
            'UDP_PORT',
            int(os.environ.get('DISTRAFT_UDP_PORT', '9000'))
        )
        setattr(
            self,
            'CLIENT_HOST',
            os.environ.get('DISTRAFT_CLIENT_HOST', '127.0.0.1').strip().lower()
        )
        setattr(
            self,
            'TCP_PORT',
            int(os.environ.get('DISTRAFT_TCP_PORT', '5000'))
        )

        # finally, the list of members of the cluster **including itself**
        # (names should be unique)
        # (comma separated) in the following format:
        # name=<self_peer_host>:<self_udp_port>:<self_client_host>:<self_tcp_port>,
        # name=<member_1_peer_host>:<member_1_udp_port>:<member_1_client_host>:<member_1_tcp_port>,
        # name=<member_2_peer_host>:<member_2_udp_port>:<member_1_client_host>:<member_2_tcp_port>,
        # ...
        # name=<member_n_host>:<member_n_udp_port>:<member_n_tcp_port>,
        members = {}
        cluster = os.environ.get('DISTRAFT_CLUSTER', None).strip().lower()
        for m in cluster.split(','):
            name, address = m.split('=')
            name = name.strip()
            mp = address.split(':')
            if members.get(name, None):
                raise Exception(f'duplicate member with name found, name: {name}')
            members.update({
                f'{name}': (mp[0].strip(), int(mp[1]), mp[2].strip(), int(mp[3]))}
            )
        setattr(self, 'MEMBERS', members)


config = Configuration()
configure = config.configure
