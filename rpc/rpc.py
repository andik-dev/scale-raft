
class MessageType(object):
    APPEND_ENTRIES = 1
    APPEND_ENTRIES_RESPONSE = 2

    REQUEST_VOTE = 3
    REQUEST_VOTE_RESPONSE = 4


class Message(object):
    def __init__(self, message_type):
        self.message_type = message_type


class RequestVoteRPC(Message):
    def __init__(self, term, candidate_id, last_log_index, last_log_term):
        Message.__init__(self, MessageType.REQUEST_VOTE)
        self.__term = term
        self.__candidateId = candidate_id
        self.__lastLogIndex = last_log_index
        self.__lastLogTerm = last_log_term


class RequestVoteRPCResponse(Message):
    def __init__(self, term, vote_granted):
        Message.__init__(self, MessageType.REQUEST_VOTE_RESPONSE)
        self.__term = term
        self.__voteGranted = vote_granted


class AppendEntriesRPCResponse(Message):
    def __init__(self, term, success):
        Message.__init__(self, MessageType.APPEND_ENTRIES_RESPONSE)
        self.__term = term
        self.__success = success


class AppendEntriesRPC(Message):
    def __init__(self, term, leader_id, prev_log_index, prev_log_term, log_entries, leader_commit_index):
        Message.__init__(self, MessageType.APPEND_ENTRIES)
        self.__term = term
        self.__leaderId = leader_id
        self.__prevLogIndex = prev_log_index
        self.__prevLogTerm = prev_log_term
        self.__logEntries = log_entries
        self.__leaderCommitIndex = leader_commit_index
