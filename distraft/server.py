import asyncio
import logging

from consensus import Raft
from quart import Quart, request, jsonify

logger = logging.getLogger(__name__)
app = Quart(__name__)

# Configuration:
NUM_SERVERS = 5
DEBUG = True
raft_server = None
servers = None


@app.before_first_request
def _setup_logging():
    fmt = '[%(asctime)s]: %(levelname)s %(message)s'
    loglevel = logging.INFO
    if DEBUG:
        loglevel = logging.DEBUG
    logging.basicConfig(format=fmt, level=loglevel)


@app.before_first_request
def _setup_consensus_networking():
    global servers
    global raft_server
    if servers:
        logger.warning("Servers already initialized")
    else:
        loop = asyncio.get_event_loop()
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
            asyncio.ensure_future(server.start(), loop=loop)


@app.route("/", methods=['GET'])
async def root():
    return "Hello World!"


@app.route('/<key>', methods=['POST'])
async def set_value(key=None):
    global raft_server
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
    global raft_server
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


@app.route("/stop")
async def stop():
    logger.info("Shutting down...")

    # stop servers:
    for server in servers:
        server.stop()

    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
