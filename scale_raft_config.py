import socket

class ScaleRaftConfig(object):
    # Global properties
    HOSTNAME = socket.gethostname()
    PORT = 47800
    MAX_CONNECTIONS = 10
    SERVER_SOCKET_TIMEOUT_IN_SECONDS = 1
    CLIENT_SOCKET_TIMEOUT_IN_SECONDS = 5

    # Local properties
    LOG_LEVEL = "DEBUG"
    LOG_FILE = "/var/log/scaleRaft.log"
