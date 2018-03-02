import socket
from threading import Thread

import logging
from time import sleep

import zlib

from rpc.messages import AppendEntries, LogEntry
from rpc.rpc_handler import RPCHandler
from rpc.serializer import ScaleRaftSerializer, AppendEntriesSerializer
from scale_raft_config import ScaleRaftConfig
from states.states import Follower

logger = logging.getLogger(__name__)


class ScaleRaftCompressor(object):
    def compress(self, data):
        return zlib.compress(data)

    def decompress(self, data):
        return zlib.decompress(data)


class ScaleRaftEncryptor(object):
    def encrypt(self, data):
        return data

    def decrypt(self, data):
        return data


class RaftServer(object):
    def __init__(self, peers, hostname=ScaleRaftConfig().HOSTNAME, port=ScaleRaftConfig().PORT,
                 compressor=ScaleRaftCompressor, encryptor=ScaleRaftEncryptor, rpc_handler=RPCHandler,
                 serializer=ScaleRaftSerializer):
        self.__peers = peers
        self.__state = Follower(self)
        self.__hostname = hostname
        self.__port = port
        self.__compressor = compressor()
        self.__encryptor = encryptor()

        #TODO move RPC handling out of this class, basic functions: read, write
        self.__rpc_handler = rpc_handler(self.__hostname, self.__port, self._msg_handler)
        self.__serializer = serializer()

        logFormat = '%(asctime)s - %(levelname)s - %(module)s - %(threadName)s - %(message)s'
        #logging.basicConfig(format=logFormat, filename=ScaleRaftConfig().LOG_FILE, level=ScaleRaftConfig().LOG_LEVEL)
        logging.basicConfig(format=logFormat, level=ScaleRaftConfig().LOG_LEVEL)

    def _msg_handler(self, string):
        obj = self._deserialize(string)
        resp_obj = self.__state.handle(obj)
        string = None
        if resp_obj is not None:
            string = self._serialize(resp_obj)
        return string

    def _serialize(self, obj):
        serialized_string = self.__serializer.serialize(obj)
        serialized_string = self.__encryptor.encrypt(serialized_string)
        serialized_string = self.__compressor.compress(serialized_string)
        return serialized_string

    def _deserialize(self, string):
        string = self.__compressor.decompress(string)
        string = self.__encryptor.decrypt(string)
        obj = self.__serializer.deserialize(string)
        return obj

    def start(self):
        logger.info("Starting server...")
        logger.info("Peers: {}".format(self.__peers))
        self.__rpc_handler.startup()
        logger.info("Server listening on {}:{}".format(self.__hostname, self.__port))

    def send(self, hostname, port, obj):
        serialized_string = self._deserialize(obj)
        self.__rpc_handler.send(hostname, port, serialized_string)

    def stop(self):
        self.__rpc_handler.shutdown()


class MyRPCHandler(RPCHandler):
    def __init__(self, hostname, port, msg_handler):
        RPCHandler.__init__(self, hostname, port, msg_handler)


class MyOtherCompressor(ScaleRaftCompressor):
    def __init__(self):
        ScaleRaftCompressor.__init__(self)


if __name__ == "__main__":
    server = RaftServer(["localhost"], compressor=MyOtherCompressor, rpc_handler=MyRPCHandler,
                        serializer=ScaleRaftSerializer)
    try:
        server.start()
        sleep(1)
    except Exception as e:
        server.stop()
        logger.exception(e)
        exit(1)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((socket.gethostname(), ScaleRaftConfig().PORT))

    #s.send(RequestVoteRPC(Message.VERSION, 2, 3, 4, 5).to_rpc_string())
    log_entries = [
        LogEntry(1, 1, 1),
        LogEntry(2, 1, "hello asdewdfasd asqfewfwefwefd wq dq sad w ad"),
        LogEntry(2, 1, "world sad w dasdiwq idquwhdquihwduqhwduqwdhqwudhquiwd"),
    ]

    #print AppendEntriesRPC(Message.VERSION, 1, 2, 3, 4, 5, log_entries).to_rpc_string()


    s.send(ScaleRaftCompressor().compress(AppendEntriesSerializer.serialize_to_string(AppendEntries(1, 2, 3, 4, 5,
                                                        log_entries))))

    s.shutdown(socket.SHUT_RDWR)
    s.close()
    server.stop()








