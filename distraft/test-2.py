import asyncio
import logging

from consensus import Raft

logger = logging.getLogger(__name__)

# Configuration:
NUM_SERVERS = 5
raft_server = None
servers = None
loop = None
TIME_TO_RUN = 5


def main_networking():
    global servers
    global raft_server
    global loop
    if servers:
        logger.warning("Servers already initialized")
    else:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        server_list = dict()
        servers = list()

        # Create Servers
        me = {
            "host": "127.0.0.1",
            "udp_port": 9000,
            "tcp_port": 5000,
            "leader": True
        }
        for i in range(1, NUM_SERVERS):
            server_list[f'127.0.0.1:{9000+i}'] = {
                "host": "127.0.0.1",
                "udp_port": 9000+i,
                "tcp_port": 5000+i,
                "leader": False
            }

        raft_server = Raft(
            host=me['host'],
            udp_port=me['udp_port'],
            tcp_port=me['tcp_port'],
            leader=me['leader'],
            members=server_list.values(),
            loop=loop
        )
        server_list['127.0.0.1:9000'] = me
        servers.append(raft_server)
        for i in range(1, NUM_SERVERS):
            m = server_list.pop(f'127.0.0.1:{9000+i}')
            servers.append(Raft(
                host='127.0.0.1',
                udp_port=9000+i,
                tcp_port=5000+i,
                members=server_list.values(),
                loop=loop
            ))
            server_list[f'127.0.0.1:{9000+i}'] = m

        # Start servers
        for server in servers:
            logger.info(f'Starting: {server.id}')
            loop.run_until_complete(server.start())


def set_value(req):
    global raft_server
    global loop

    try:
        if req['type'].strip().lower() in ['set']:
            loop.run_until_complete(raft_server.set_value(req['key'].strip(), req['value']))
            return {'ok': True}
        else:
            return {'ok': False, 'error': f'unknown request type {req["type"]}'}
    except Exception as err:
        logger.error(f'Error in set_value(): {str(err)}')
        return {'ok': False, 'error': f'system error: {str(err)}'}


def stop():
    logger.info("Shutting down...")

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

    # loop for a while to allow for emtpy heartbeats
    loop.run_until_complete(asyncio.sleep(TIME_TO_RUN, loop=loop))

    # send some messages:
    set_value({'type': 'set', 'key': 'a', 'value': 1})
    set_value({'type': 'set', 'key': 'a', 'value': 2})
    set_value({'type': 'set', 'key': 'a', 'value': 3})

    loop.run_until_complete(asyncio.sleep(TIME_TO_RUN, loop=loop))

    # stop servers:
    stop()
