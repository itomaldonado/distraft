import asyncio
import logging

from consensus import Raft
from quart import Quart, request, jsonify

logger = logging.getLogger(__name__)
app = Quart(__name__)

# Configuration:
NUM_SERVERS = 5
DEBUG = True
servers = None
loop = None


def _setup_logging():
    fmt = '[%(asctime)s]: %(levelname)s %(message)s'
    loglevel = logging.INFO
    if DEBUG:
        loglevel = logging.DEBUG
    logging.basicConfig(format=fmt, level=loglevel)


def _setup_consensus_networking():
    global servers
    global loop
    if servers:
        logger.warning("Servers already initialized")
    else:
        loop = asyncio.get_event_loop()
        server_list = dict()
        servers = dict()

        # Create Servers
        for i in range(0, NUM_SERVERS):
            m = ("127.0.0.1", 9000+i, 5000+i)
            server_list.update({f'{m[0]}:{m[1]}:{m[2]}': m})

        for i in range(0, NUM_SERVERS):
            m = server_list.pop(f'127.0.0.1:{9000+i}:{5000+i}')
            servers.update(
                {f'127.0.0.1:{9000+i}:{5000+i}': Raft(
                    host='127.0.0.1',
                    udp_port=9000+i,
                    tcp_port=5000+i,
                    members=server_list.values(),
                    loop=loop)}
            )
            server_list.update({f'127.0.0.1:{9000+i}:{5000+i}': m})

        # Start servers
        for server_id, server in servers.items():
            logger.info(f'Starting: {server_id}')
            asyncio.ensure_future(server.start(), loop=loop)


def get_leader():
    global servers
    for server_id, server in servers.items():
        if (server.leader and server.up):
            return servers[server.leader]


@app.route("/", methods=['GET'])
async def root():
    return "Hello World!"


@app.route('/<key>', methods=['POST'])
async def set_value(key=None):
    raft_server = get_leader()
    try:
        form = await request.form
        if (key and key.strip()) and (form['value'] and form['value'].strip()):
            k = key.strip()
            v = form['value'].strip()
            await raft_server.set_value(k, v)
            return jsonify({'ok': True, "key": k, "value": v}), 200
        else:
            return jsonify({'ok': False, 'error': f'empty key or value'}), 503
    except Exception as err:
        logger.error(f'Error in set_value(): {str(err)}')
        return jsonify({'ok': False, 'error': f'system error: {str(err)}'}), 500


@app.route('/<key>', methods=['GET'])
async def get_value(key=None):
    raft_server = get_leader()
    try:
        if key and key.strip():
            k = key.strip()
            v = await raft_server.get_value(k)
            return jsonify({'ok': True, "key": k, "value": v}), 200
        else:
            return jsonify({'ok': False, 'error': f'empty key'}), 503
    except Exception as err:
        logger.error(f'Error in set_value(): {str(err)}')
        return jsonify({'ok': False, 'error': f'{str(err)}'}), 500


@app.route('/status', methods=['GET'])
async def get_server_status(key=None):
    global servers
    status = {}
    leader = get_leader()
    leader = leader.id if leader else None
    status.update(
        {
            'leader': leader,
            'members': [
                {
                    'id': m.id,
                    'leader': m.leader,
                    'state': m.state.name,
                    'commit_index': m.log.commit_index,
                    'last_applied': m.log.last_applied,
                    'last_log_index': m.log.last_log_index,
                    'last_log_term': m.log.last_log_term,
                    'status': m.up,
                    'term': m.state_storage.get('term')

                } for m in servers.values()
            ]
        }
    )
    return jsonify({'ok': True, 'status': status}), 200


@app.route("/stop/<server_id>")
async def stop(server_id):
    logger.info(f'Shutting down server: {server_id}...')
    global servers

    # stop server
    server = servers.get(server_id)
    logger.debug(f'Got raft server: {server.id}')
    if (server and server.up):
        server.stop()

    return jsonify({'ok': True}), 200


@app.route("/start/<server_id>")
async def start(server_id):
    logger.info(f'Starting server: {server_id}...')
    global servers
    global loop

    # stop server
    server = servers.get(server_id)
    if (server and not server.up):
        asyncio.ensure_future(server.start(), loop=loop)

    return jsonify({'ok': True}), 200


if __name__ == '__main__':
    _setup_logging()
    _setup_consensus_networking()
    app.run(host='127.0.0.1', port=5000, debug=True)
