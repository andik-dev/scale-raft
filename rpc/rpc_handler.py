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
        self.__server_socket.settimeout(ScaleRaftConfig().SERVER_SOCKET_TIMEOUT_IN_SECONDS)
        self.__message_loop_thread = Thread(target=self._message_loop)

    def startup(self):
        self.__server_socket.bind((self.__hostname, self.__port))
        self.__server_socket.listen(ScaleRaftConfig().MAX_CONNECTIONS)
        self.__message_loop_thread.start()

    def shutdown(self):
        self.__shutdown = True
        for t in self.__client_threads:
            print "t={}, isAlive()={}".format(t.getName(), t.isAlive())

    def send(self, hostname, port, string):
        cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cs.settimeout(ScaleRaftConfig().CLIENT_SOCKET_TIMEOUT_IN_SECONDS)
        cs.connect((hostname, port))

        self._send(cs, string)
        cs.shutdown(socket.SHUT_WR)
        string = self._recv(cs)
        #cs.shutdown(socket.SHUT_RD)
        cs.close()
        return string

    def _message_loop(self):
        while not self.__shutdown:
            try:
                (client_socket, client_address) = self.__server_socket.accept()
                logger.debug("Handling request from: {}:{}".format(client_address[0], client_address[1]))
                t = Thread(target=self._handle_new_connection, args=(client_socket,))
                self.__client_threads.append(t)
                t.start()
            except socket.timeout:
                pass

    def _handle_new_connection(self, client_socket):
        msg = self._recv(client_socket)
        resp = self.__msg_handler(msg)
        if resp is not None:
            self._send(client_socket, resp)
        client_socket.shutdown(socket.SHUT_RDWR)
        client_socket.close()
        return msg

    @staticmethod
    def _send(client_socket, string):
        # client_socket.sendall(string)
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
            chunks.append(chunk)
        string = b''.join(chunks)
        return string



