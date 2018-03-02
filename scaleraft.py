import socket
from threading import Thread

import logging

from rpc.rpc import Message, AppendEntriesRPC, RequestVoteRPC
from scale_raft_config import ScaleRaftConfig
from states.states import Follower

logger = logging.getLogger(__name__)

class RaftServer(object):
    def __init__(self, peers, hostname=ScaleRaftConfig().HOSTNAME, port=ScaleRaftConfig().PORT):
        self.__peers = peers
        self.__state = Follower(self)

        self.__server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.__port = port

        self.__message_loop_thread = Thread(target=self._message_loop)
        self.__stop = False

        self.__client_threads = []

        self.__hostname = hostname

        logFormat = '%(asctime)s - %(levelname)s - %(module)s - %(threadName)s - %(message)s'
        #logging.basicConfig(format=logFormat, filename=ScaleRaftConfig().LOG_FILE, level=ScaleRaftConfig().LOG_LEVEL)
        logging.basicConfig(format=logFormat, level=ScaleRaftConfig().LOG_LEVEL)

    def start(self):
        logger.info("Starting server...")
        logger.info("Peers: {}".format(self.__peers))
        self.__server_socket.bind((self.__hostname, self.__port))
        self.__server_socket.listen(ScaleRaftConfig().MAX_CONNECTIONS)
        self.__message_loop_thread.start()
        logger.info("Server listening on {}:{}".format(self.__hostname, self.__port))

    def send(self, message):
        print message

    def _handle_new_connection(self, client_socket, address):
        logger.debug("Handling request from: {}:{}".format(address[0], address[1]))
        bufsize = 4096
        chunks = []
        while True:
            chunk = client_socket.recv(bufsize)
            if len(chunk) == 0:
                break
            chunks.append(chunk)
        message = b''.join(chunks)
        logger.debug("Received {} bytes: {}".format(len(message), message))
        parsed_msg = Message.parse_message(message)
        self.__state.handle(parsed_msg)
        client_socket.shutdown(socket.SHUT_RDWR)
        client_socket.close()
        return

    def stop(self):
        self.__stop = True

    def _message_loop(self):
        while not self.__stop:
            (client_socket, address) = self.__server_socket.accept()
            t = Thread(target=self._handle_new_connection, args=(client_socket, address))
            self.__client_threads.append(t)
            t.start()


if __name__ == "__main__":
    server = RaftServer(["localhost"])
    try:
        server.start()
    except Exception as e:
        server.stop()
        logger.exception(e)
        exit(1)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((socket.gethostname(), ScaleRaftConfig().PORT))

    s.send(RequestVoteRPC(Message.VERSION, 2, 3, 4, 5).to_rpc_string())

    s.shutdown(socket.SHUT_RDWR)
    s.close()
    server.stop()








