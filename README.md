# raft-py
A just for fun implementation of the Raft consensus algorithm in Python.

## Sample output

Three processes are started on the local node each using a different hostname/IP for demonstration purposes.
After the (configurable) election timeout expired a new leader is chosen.

```
andi@andi-vbox:~$ python git/raft-py/raft_server.py --test
2019-10-29 21:45:26,339 - INFO - raft_server - MainThread - Starting server...
2019-10-29 21:45:26,339 - INFO - raft_server - MainThread - Peers: [('localhost', 48001), ('andi-vbox', 48002)]
2019-10-29 21:45:26,340 - INFO - raft_server - MainThread - 2 peers configured, starting as Follower
2019-10-29 21:45:26,340 - INFO - raft_server - MainThread - Server listening on 127.0.0.1:48000
2019-10-29 21:45:26,341 - INFO - raft_server - MainThread - Starting server...
2019-10-29 21:45:26,341 - INFO - raft_server - MainThread - Peers: [('127.0.0.1', 48000), ('localhost', 48001)]
2019-10-29 21:45:26,341 - INFO - raft_server - MainThread - 2 peers configured, starting as Follower
2019-10-29 21:45:26,344 - INFO - raft_server - MainThread - Server listening on andi-vbox:48002
2019-10-29 21:45:26,345 - INFO - raft_server - MainThread - Starting server...
2019-10-29 21:45:26,345 - INFO - raft_server - MainThread - Peers: [('andi-vbox', 48002), ('127.0.0.1', 48000)]
2019-10-29 21:45:26,345 - INFO - raft_server - MainThread - 2 peers configured, starting as Follower
2019-10-29 21:45:26,346 - INFO - raft_server - MainThread - Server listening on localhost:48001
2019-10-29 21:45:28,024 - INFO - raft_server - andi-vbox - Thread-4 - andi-vbox: No valid RPC received in the last 1687 milliseconds, switching to Candidate
2019-10-29 21:45:28,030 - INFO - states - andi-vbox - Thread-7 - andi-vbox: Starting vote...
2019-10-29 21:45:28,030 - INFO - raft_server - andi-vbox - Thread-4 - andi-vbox: Switching state from Follower to Candidate
2019-10-29 21:45:28,038 - INFO - states - Thread-9 - 127.0.0.1: Voting for andi-vbox
2019-10-29 21:45:28,038 - INFO - states - Thread-9 - 127.0.0.1: Old Leader: None, new leader: None
2019-10-29 21:45:28,039 - INFO - states - Thread-9 - 127.0.0.1: Voted for andi-vbox
2019-10-29 21:45:28,039 - INFO - states - andi-vbox - Thread-8 - andi-vbox: Received 2 votes
2019-10-29 21:45:28,039 - INFO - states - andi-vbox - Thread-8 - andi-vbox: Received 2 votes. Switching to Leader
2019-10-29 21:45:28,039 - INFO - states - andi-vbox - Thread-8 - andi-vbox: Old Leader: None, new leader: andi-vbox
2019-10-29 21:45:28,042 - INFO - states - Thread-12 - localhost: Voting for andi-vbox
2019-10-29 21:45:28,043 - INFO - raft_server - andi-vbox - Thread-8 - andi-vbox: Switching state from Candidate to Leader
2019-10-29 21:45:28,043 - INFO - states - Thread-12 - localhost: Old Leader: None, new leader: None
2019-10-29 21:45:28,043 - INFO - states - Thread-12 - localhost: Voted for andi-vbox
2019-10-29 21:45:28,045 - INFO - states - Thread-15 - 127.0.0.1: New leader: andi-vbox
2019-10-29 21:45:28,045 - INFO - states - Thread-15 - 127.0.0.1: Old Leader: None, new leader: andi-vbox
2019-10-29 21:45:28,046 - INFO - states - Thread-16 - localhost: New leader: andi-vbox
2019-10-29 21:45:28,046 - INFO - states - Thread-16 - localhost: Old Leader: None, new leader: andi-vbox
2019-10-29 21:46:38,410 - WARNING - raft_server - andi-vbox - Thread-2674 - andi-vbox: It took 136ms to send a message to and receive a response from: localhost:48001
2019-10-29 21:46:38,411 - WARNING - raft_server - andi-vbox - Thread-2670 - andi-vbox: It took 238ms to send a message to and receive a response from: localhost:48001

```
