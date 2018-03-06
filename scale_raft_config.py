import socket

class ScaleRaftConfig(object):
    # Global properties
    HOSTNAME = socket.gethostname()
    PORT = 47800
    MAX_CONNECTIONS = 10
    SERVER_SOCKET_TIMEOUT_IN_SECONDS = 5
    CLIENT_SOCKET_TIMEOUT_IN_SECONDS = 10

    # Raft specific properties
    ELECTION_TIMEOUT_IN_MILLIS_MIN = 1500
    ELECTION_TIMEOUT_IN_MILLIS_MAX = 3000


    # Local properties
    LOG_LEVEL = "DEBUG"
    LOG_FILE = "/var/log/scaleRaft.log"
