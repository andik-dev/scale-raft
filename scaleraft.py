import random
import socket
from threading import Thread

from scale_raft_config import ScaleRaftConfig
from states.states import Follower


class RaftServer(object):
    def __init__(self, servers, port=ScaleRaftConfig().PORT):
        self.__servers = servers
        self.__state = Follower(self)

        self.__server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.__port = port

        self.__message_loop_thread = Thread(target=self._message_loop)
        self.__stop = False

        self.__client_threads = []

    def start(self):
        self.__server_socket.bind((socket.gethostname(), self.__port))
        self.__server_socket.listen(ScaleRaftConfig().MAX_CONNECTIONS)
        self.__message_loop_thread.start()

    def send(self, message):
        print message

    def _handle_new_connection(self, client_socket, address):
        print "Handling request from: {}".format(address)

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
    server.start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print socket.gethostname()
    s.connect((socket.gethostname(), ScaleRaftConfig().PORT))
    s.send("hello world")
    s.shutdown(socket.SHUT_RDWR)
    s.close()

    server.stop()







