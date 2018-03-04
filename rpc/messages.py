class MessageType(object):
    APPEND_ENTRIES = 1
    APPEND_ENTRIES_RESPONSE = 2

    REQUEST_VOTE = 3
    REQUEST_VOTE_RESPONSE = 4

    CLIENT_DATA = 5
    CLIENT_DATA_RESPONSE = 6


class BaseMessage(object):
    VERSION = 1

    def __init__(self, message_type):
        self.version = BaseMessage.VERSION
        self.message_type = int(message_type)


class RequestVote(BaseMessage):
    def __init__(self, term, candidate_id, last_log_index, last_log_term):
        BaseMessage.__init__(self, MessageType.REQUEST_VOTE)
        self.term = int(term)
        self.candidateId = str(candidate_id)
        self.lastLogIndex = int(last_log_index)
        self.lastLogTerm = int(last_log_term)


class RequestVoteResponse(BaseMessage):
    def __init__(self, term, vote_granted):
        BaseMessage.__init__(self, MessageType.REQUEST_VOTE_RESPONSE)
        self.term = int(term)
        self.voteGranted = bool(vote_granted)


class AppendEntriesResponse(BaseMessage):
    def __init__(self, term, success):
        BaseMessage.__init__(self, MessageType.APPEND_ENTRIES_RESPONSE)
        self.term = int(term)
        self.success = bool(success)


class AppendEntries(BaseMessage):
    def __init__(self, term, leader_id, prev_log_index, prev_log_term, leader_commit_index, log_entries):
        BaseMessage.__init__(self, MessageType.APPEND_ENTRIES)
        self.term = int(term)
        self.leaderId = str(leader_id)
        self.prevLogIndex = int(prev_log_index)
        self.prevLogTerm = int(prev_log_term)
        self.leaderCommitIndex = int(leader_commit_index)
        self.logEntries = log_entries


class LogEntry(object):
    def __init__(self, index, term, data):
        self.index = int(index)
        self.term = int(term)
        self.data = str(data)


class ClientData(BaseMessage):
    def __init__(self, data):
        BaseMessage.__init__(self, MessageType.CLIENT_DATA)
        self.data = str(data)


class ClientDataResponse(BaseMessage):
    def __init__(self, success, leader_id):
        BaseMessage.__init__(self, MessageType.CLIENT_DATA_RESPONSE)
        self.success = bool(success)
        self.leaderId = str(leader_id)


