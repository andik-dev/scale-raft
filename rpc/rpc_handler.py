import logging
import socket
from threading import Thread

from scale_raft_config import ScaleRaftConfig

logger = logging.getLogger(__name__)

class RPCHandler(object):
    def __init__(self, hostname, port, msg_handler):
        self.__msg_handler = msg_handler
        self.__hostname = hostname
        self.__port = port
        self.__shutdown = False
        self.__client_threads = []
        self.__server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__message_loop_thread = Thread(target=self._message_loop)

    def startup(self):
        self.__server_socket.bind((self.__hostname, self.__port))
        self.__server_socket.listen(ScaleRaftConfig().MAX_CONNECTIONS)
        self.__message_loop_thread.start()

    def shutdown(self):
        self.__shutdown = True

    def send(self, hostname, port, string):
        cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cs.connect((hostname, port))

        self._send(cs, string)
        string = self._recv(cs)

        cs.shutdown(socket.SHUT_RDWR)
        cs.close()
        return string

    def _message_loop(self):
        while not self.__shutdown:
            (client_socket, client_address) = self.__server_socket.accept()
            logger.debug("Handling request from: {}:{}".format(client_address[0], client_address[1]))
            t = Thread(target=self._handle_new_connection, args=(client_socket,))
            self.__client_threads.append(t)
            t.start()

    def _handle_new_connection(self, client_socket):
        msg = self._recv(client_socket)
        resp = self.__msg_handler(msg)
        if resp is not None:
            self._send(client_socket, resp)
        client_socket.shutdown(socket.SHUT_RDWR)
        client_socket.close()
        return msg

    @staticmethod
    def _send(socket, string):
        socket.sendall(string)

    @staticmethod
    def _recv(socket):
        buf_size = 4096
        chunks = []
        while True:
            chunk = socket.recv(buf_size)
            if len(chunk) == 0:
                break
            chunks.append(chunk)
        string = b''.join(chunks)
        return string



