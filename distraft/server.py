import asyncio
import logging

from consensus import Raft
from flask import Flask, request, jsonify

logger = logging.getLogger(__name__)
app = Flask(__name__)

# Configuration:
NUM_SERVERS = 5
raft_server = None
servers = None
loop = None


@app.before_first_request
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
        server_list['127.0.0.1:9000'] = me
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
            loop=loop
        )
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


@app.route("/")
def hello():
    return "Hello World!"


@app.route('/set', methods=['POST'])
def set_value():
    global raft_server
    global loop
    req = request.get_json()
    try:
        if req['type'].strip().lower() in ['set']:
            loop.run_until_complete(raft_server.set_value(req['key'].strip(), req['value'].strip()))
            return jsonify({'ok': True}), 200
        else:
            return jsonify({'ok': False, 'error': f'unknown request type {req["type"]}'}), 503
    except Exception as err:
        logger.error(f'Error in set_value(): {err.message}')
        return jsonify({'ok': False, 'error': f'system error: {err.message}'}), 500


@app.route("/stop")
def stop():
    logger.info("Shutting down...")

    # stop servers:
    for server in servers:
        server.stop()

    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


def _setup_logging(debug):
    fmt = '[%(asctime)s]: %(levelname)s %(message)s'
    loglevel = logging.INFO
    if debug:
        loglevel = logging.DEBUG
    logging.basicConfig(format=fmt, level=loglevel)


if __name__ == '__main__':
    _setup_logging(True)
    app.run(host='127.0.0.1', port=5000, debug=True)
