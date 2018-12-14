import asyncio
import logging
import urlparse

from config import config
from consensus import Raft
from quart import Quart, request, jsonify, redirect

logger = logging.getLogger(__name__)
app = Quart(__name__)

# globals
raft = None
loop = None


def _setup_logging():
    fmt = '[%(asctime)s]: %(levelname)s %(message)s'
    logging.basicConfig(format=fmt, level=config.LOG_LEVEL)


def _setup_consensus_networking():
    global raft
    global loop

    loop = asyncio.get_event_loop()

    # Create the raft consensus server object
    raft = Raft(
        node_id=config.NAME,
        host=config.HOST,
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


@app.route("/", methods=['GET'])
async def root():
    global raft
    return jsonify({'ok': True, 'raft_status': f'{raft.up}'}), 200


@app.route('/<key>', methods=['POST'])
async def set_value(key=None):
    global raft
    if is_leader():
        try:
            form = await request.form
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
        leader = get_leader()
        if leader:
            leader_info = config.MANAGERS[leader]
            parsed = urlparse.urlparse(request.url)
            replaced = parsed._replace(netloc=f'{leader_info[0]}:{leader_info[2]}')
            return redirect(urlparse.urlunparse(replaced), code=302)
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
        leader = get_leader()
        if leader:
            leader_info = config.MANAGERS[leader]
            parsed = urlparse.urlparse(request.url)
            replaced = parsed._replace(netloc=f'{leader_info[0]}:{leader_info[2]}')
            return redirect(urlparse.urlunparse(replaced), code=302)
        else:  # if no leader, error out...
            return jsonify({'ok': False, "error": 'no leader'}), 503


@app.route('/status', methods=['GET'])
async def get_server_status(key=None):
    global raft
    status = {}
    status.update(
        {
            'id': raft.id,
            'leader': raft.leader,
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
async def stop():
    global raft
    logger.info(f'{raft.id} starting up the raft server.')

    # start server
    if not raft.up:
        raft.start()

    return jsonify({'ok': True}), 200


if __name__ == '__main__':
    _setup_logging()
    _setup_consensus_networking()

    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = config.PRETTY_PRINT_RESPONSES
    app.run(host='127.0.0.1', port=5000, debug=config.WEB_SERVER_DEBUG)
