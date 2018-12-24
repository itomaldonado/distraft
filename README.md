# distraft
Raft implementation in a distributed fashion

# Installation:

To install distraft you need to follow the steps below:

## Pre-reqs

* Python 3.7+
* [pip](https://pip.pypa.io/en/stable/installing/)
* (Optional) [virtualenv](https://virtualenv.pypa.io/en/latest/)
* (Optional, for example cluster) [Docker](https://docs.docker.com/)
* (Optional, for example cluster) [docker-compose](https://docs.docker.com/compose/install/)

## Install steps:

* (Optional) Create and activate a virtual environment: `virtalenv ./venv/ && source ./venv/bin/activate`
* Install dependencies: `pip install -r requirements.txt`

## Run server:

All server configuration is done via environment variables, before running the server, modify the
file:  `distraft-config.env` to configure your server then to start the server run the following commands:

```
# Apply the environmental configurations:
source ./distraft-config.env

# Move to the distraft server:
cd ./distraft

# Start the server:
python server.py
```

# Run in Docker

Running in docker makes the bootstrapping of a 5-node cluster extremely easy, just make sure you have `docker` and `docker-compose` installed then run the following command:

```
# Start the 5-node cluster:
docker-compose up -d
```

# Using the Server:

Once the server is running here are some of the HTTP requests/commands you can use to interact with the server:

* Get the status of a specific node (e.g. 127.0.0.1:5001)
    * `curl http://127.0.0.1:5001/`
    * Returns a json message like the following:
    ```
    {
      "ok": true,
      "status": {
        "commit_index": 0,
        "id": "server1",
        "is_leader": false,
        "last_applied": 0,
        "last_log_index": 0,
        "last_log_term": 0,
        "leader": "default",
        "match_index": {},
        "members": {
          "default": [
            "127.0.0.1",
            9000,
            "127.0.0.1",
            5000
          ],
          "server1": [
            "127.0.0.1",
            9001,
            "127.0.0.1",
            5001
          ],
          "server2": [
            "127.0.0.1",
            9002,
            "127.0.0.1",
            5002
          ]
        },
        "next_index": {},
        "state": "Follower",
        "term": 1,
        "up": true
      }
    }
    ```
* Get the value of a key:
    * `curl -L http://127.0.0.1:5001/<key>`
    * Returns a json message like the following:
    ```
    {
        "ok": true,
        "key": "<key>",
        "value": "<value>"
    }
    ```

* Set the value of a key:
    * `curl -L -XPOST --data 'value=<new_value>' http://127.0.0.1:5001/<key>`
    * Returns a json message like the following:
    ```
    {
        "ok": true,
        "key": "<key>",
        "value": "<new_value>"
    }
    ```

* Stop a node (for testing purposes):
    * `curl http://127.0.0.1:5001/stop`
    * Returns a json message like the following:
    ```
    {"ok": true}
    ```

* Start a node (for testing purposes):
    * `curl http://127.0.0.1:5001/start`
    * Returns a json message like the following:
    ```
    {"ok": true}
    ```
