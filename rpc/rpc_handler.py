import logging
import socket
from threading import Thread
from time import sleep

from scale_raft_config import ScaleRaftConfig

logger = logging.getLogger(__name__)


class RPCHandler(object):
    def __init__(self, hostname, port, msg_handler, timeout_handler):
        self._msg_handler = msg_handler
        self._timeout_handler = timeout_handler
        self.__hostname = hostname
        self.__port = port
        self.__shutdown = False
        self.__client_threads = []
        self.__server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__server_socket.settimeout(ScaleRaftConfig().SERVER_SOCKET_TIMEOUT_IN_SECONDS)
        self.__message_loop_thread = Thread(target=self._message_loop)

    def startup(self):
        self.__server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.__server_socket.bind((self.__hostname, self.__port))
        self.__server_socket.listen(ScaleRaftConfig().MAX_CONNECTIONS)
        self.__message_loop_thread.start()

    def shutdown(self):
        logger.info("Shutting down...")
        self.__shutdown = True
        while self.__message_loop_thread.is_alive():
            pass
        for t in self.__client_threads:
            while t.is_alive():
                pass
        self.__server_socket.close()
        logger.info("Finished.")

    def send(self, hostname, port, string):
        sent = False
        while not sent and not self.__shutdown:
            try:
                cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                cs.settimeout(ScaleRaftConfig().CLIENT_SOCKET_TIMEOUT_IN_SECONDS)
                cs.connect((hostname, port))

                self._send(cs, string)
                sent = True
                string = self._recv(cs)
                cs.close()
                return string
            except Exception as e:
                logger.error(str(e))
                sleep(1)

    def _message_loop(self):
        while not self.__shutdown:
            try:
                (client_socket, client_address) = self.__server_socket.accept()
                logger.debug("Handling request from: {}:{}".format(client_address[0], client_address[1]))
                t = Thread(target=self._handle_new_connection, args=(client_socket,))
                self.__client_threads.append(t)
                t.start()
            except socket.timeout:
                self._timeout_handler()

    def _handle_new_connection(self, client_socket):
        string = self._recv(client_socket)
        resp = self._msg_handler(string)
        if resp is not None:
            self._send(client_socket, resp)
        client_socket.close()
        return string

    @staticmethod
    def _send(client_socket, string):
        string += '\0'
        bytes_sent = 0
        while bytes_sent < len(string):
            bytes_sent += client_socket.send(string[bytes_sent:])

    @staticmethod
    def _recv(client_socket):
        buf_size = 4096
        chunks = []
        while True:
            chunk = client_socket.recv(buf_size)
            if len(chunk) == 0:
                break
            if chunk[-1] == '\0':
                chunks.append(chunk[0:-1])
                break
            chunks.append(chunk)

        string = b''.join(chunks)
        return string



