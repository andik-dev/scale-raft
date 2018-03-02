import logging

logger = logging.getLogger(__name__)


class MessageType(object):
    APPEND_ENTRIES = 1
    APPEND_ENTRIES_RESPONSE = 2

    REQUEST_VOTE = 3
    REQUEST_VOTE_RESPONSE = 4


class InvalidMessageVersionException(Exception):
    def __init__(self, expected, actual):
        self.expected = expected
        self.actual = actual

    def __str__(self):
        return "Invalid message version: Expected {}, received {}".format(self.expected,self.actual)


class Message(object):
    VERSION = 1

    def __init__(self, version, message_type):
        self.message_type = int(message_type)
        self.version = int(version)

    @staticmethod
    def parse_message(message):
        # version;message_type;PAYLOAD
        (version, message_type, payload) = message.split(";", 2)
        version = int(version)
        message_type = int(message_type)
        if version != Message.VERSION:
            raise InvalidMessageVersionException(Message.VERSION, version)
        msg = None
        if message_type == MessageType.REQUEST_VOTE:
            logger.info("Received REQUEST_VOTE rpc")
            msg = RequestVoteRPC.from_rpc_string(version, payload)

        logger.debug("Received message: {}".format(msg))
        return msg

    def to_rpc_string(self):
        return "{};{}".format(Message.VERSION, self.message_type)


class RequestVoteRPC(Message):
    def __init__(self, version, term, candidate_id, last_log_index, last_log_term):
        Message.__init__(self, version, MessageType.REQUEST_VOTE)
        self.__term = int(term)
        self.__candidateId = int(candidate_id)
        self.__lastLogIndex = int(last_log_index)
        self.__lastLogTerm = int(last_log_term)

    def to_rpc_string(self):
        return "{};{};{};{};{}".format(Message.to_rpc_string(self), self.__term, self.__candidateId, self.__lastLogIndex, self.__lastLogTerm)

    @staticmethod
    def from_rpc_string(version, payload):
        (term, candidate_id, last_log_index, last_log_term) = payload.split(";", 4)
        return RequestVoteRPC(version, term, candidate_id, last_log_index, last_log_term)


class RequestVoteRPCResponse(Message):
    def __init__(self, version, term, vote_granted):
        Message.__init__(self, version, MessageType.REQUEST_VOTE_RESPONSE)
        self.__term = int(term)
        self.__voteGranted = bool(vote_granted)

    def to_rpc_string(self):
        return "{};{};{}".format(Message.to_rpc_string(self), self.__term, self.__voteGranted)

    @staticmethod
    def from_rpc_string(version, payload):
        (term, vote_granted) = payload.split(";", 1)
        return RequestVoteRPCResponse(version, term, vote_granted)


class AppendEntriesRPCResponse(Message):
    def __init__(self, version, term, success):
        Message.__init__(self, version, MessageType.APPEND_ENTRIES_RESPONSE)
        self.__term = term
        self.__success = success

    def to_rpc_string(self):
        return "{};{};{};{};{}".format(Message.to_rpc_string(self), self.__term, self.__candidateId, self.__lastLogIndex, self.__lastLogTerm)

    @staticmethod
    def from_rpc_string(version, payload):
        (term, candidate_id, last_log_index, last_log_term) = payload.split(";", 4)
        return RequestVoteRPC(version, term, candidate_id, last_log_index, last_log_term)


class AppendEntriesRPC(Message):
    def __init__(self, version, term, leader_id, prev_log_index, prev_log_term, log_entries, leader_commit_index):
        Message.__init__(self, version, MessageType.APPEND_ENTRIES)
        self.__term = term
        self.__leaderId = leader_id
        self.__prevLogIndex = prev_log_index
        self.__prevLogTerm = prev_log_term
        self.__logEntries = log_entries
        self.__leaderCommitIndex = leader_commit_index

    def to_rpc_string(self):
        return "{};{};{};{};{}".format(Message.to_rpc_string(self), self.__term, self.__candidateId, self.__lastLogIndex, self.__lastLogTerm)

    @staticmethod
    def from_rpc_string(version, payload):
        (term, candidate_id, last_log_index, last_log_term) = payload.split(";", 4)
        return RequestVoteRPC(version, term, candidate_id, last_log_index, last_log_term)
