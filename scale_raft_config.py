import socket

class ScaleRaftConfig(object):
    # Global properties
    HOSTNAME = socket.gethostname()
    PORT = 47800
    MAX_CONNECTIONS = 10

    # Local properties
    LOG_LEVEL = "DEBUG"
    LOG_FILE = "/var/log/scaleRaft.log"
