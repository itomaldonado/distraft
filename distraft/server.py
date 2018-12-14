import asyncio
import logging
import sys

from config import config
from consensus import Raft
from quart import Quart, request, jsonify, redirect
from urllib.parse import urlparse, urlunparse
from quart_cors import cors

logger = logging.getLogger(__name__)
app = Quart(__name__)
app = cors(app)

# globals
raft = None
loop = None


def _setup_logging():
    fmt = '[%(asctime)s]: %(levelname)s %(message)s'
    logging.basicConfig(format=fmt, level=config.LOG_LEVEL, stream=sys.stdout)


def _setup_consensus_networking():
    global raft
    global loop

    loop = asyncio.get_event_loop()

    # Create the raft consensus server object
    raft = Raft(
        node_id=config.NAME,
        host=config.HOST,
        client_host=config.CLIENT_HOST,
        udp_port=config.UDP_PORT,
        tcp_port=config.TCP_PORT,
        members=config.MEMBERS,
        loop=loop
    )

    # Start the raft consensus server
    asyncio.ensure_future(raft.start(), loop=loop)


def get_leader():
    global raft
    if raft.leader and raft.up:
        return raft.leader
    else:
        return None


def is_leader():
    global raft
    if raft.leader and raft.up and raft.leader == raft.id:
        return True
    else:
        return False


@app.route('/<key>', methods=['POST'])
async def set_value(key=None):
    global raft
    form = await request.form
    logger.debug(f'{raft.id} post message received, set "{key}" -> "{form}".')
    if is_leader():
        try:
            if (key and key.strip()) and (form['value'] and form['value'].strip()):
                k = key.strip()
                v = form['value'].strip()
                await raft.set_value(k, v)
                return jsonify({'ok': True, "key": k, "value": v}), 200
            else:
                return jsonify({'ok': False, 'error': f'empty key or value'}), 503
        except Exception as err:
            logger.error(f'Error in set_value(): {str(err)}')
            return jsonify({'ok': False, 'error': f'system error: {str(err)}'}), 503
    else:
        # not the leader, redirect **if posible**
        leader_address = raft.leader_client_address
        if leader_address:
            parsed = urlparse(request.url)
            replaced = parsed._replace(netloc=leader_address)
            return redirect(urlunparse(replaced), status_code=307)
        else:  # if no leader, error out...
            return jsonify({'ok': False, "error": 'no leader'}), 503


@app.route('/<key>', methods=['GET'])
async def get_value(key=None):
    global raft
    if is_leader():
        try:
            if key and key.strip():
                k = key.strip()
                v = await raft.get_value(k)
                return jsonify({'ok': True, "key": k, "value": v}), 200
            else:
                return jsonify({'ok': False, 'error': f'empty key'}), 503
        except Exception as err:
            logger.error(f'Error in set_value(): {str(err)}')
            return jsonify({'ok': False, 'error': f'{str(err)}'}), 503
    else:
        # not the leader, redirect **if posible**
        leader_address = raft.leader_client_address
        if leader_address:
            parsed = urlparse(request.url)
            replaced = parsed._replace(netloc=leader_address)
            return redirect(urlunparse(replaced))
        else:  # if no leader, error out...
            return jsonify({'ok': False, "error": 'no leader'}), 503


@app.route('/', methods=['GET'])
async def get_server_status(key=None):
    global raft
    status = {}
    status.update(
        {
            'id': raft.id,
            'leader': raft.leader,
            'is_leader': (raft.leader == raft.id),
            'state': raft.state.name,
            'up': raft.up,
            'commit_index': raft.log.commit_index,
            'last_applied': raft.log.last_applied,
            'last_log_index': raft.log.last_log_index,
            'last_log_term': raft.log.last_log_term,
            'term': raft.state_storage.get('term'),
            'next_index': raft.log.next_index if raft.state.name == 'Leader' else {},
            'match_index': raft.log.next_index if raft.state.name == 'Leader' else {},
            'members': config.MEMBERS
        }
    )
    return jsonify({'ok': True, 'status': status}), 200


@app.route("/stop")
async def stop():
    global raft
    logger.info(f'{raft.id} shutting down the raft server.')

    # stop server
    if raft.up:
        raft.stop()

    return jsonify({'ok': True}), 200


@app.route("/start")
async def start():
    global raft
    global loop
    logger.info(f'{raft.id} starting up the raft server.')

    # start server
    if not raft.up:
        asyncio.ensure_future(raft.start(), loop=loop)

    return jsonify({'ok': True}), 200


if __name__ == '__main__':
    _setup_logging()
    _setup_consensus_networking()

    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = config.PRETTY_PRINT_RESPONSES
    app.run(host=config.CLIENT_HOST, port=config.TCP_PORT, debug=config.WEB_SERVER_DEBUG)
