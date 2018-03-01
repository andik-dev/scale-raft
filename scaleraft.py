
class AppendEntriesRPCResponse(object):
    def __init__(self, term, success):
        self.__term = term
        self.__success = success


class AppendEntriesRPC(object):
    def __init__(self, term, leaderId, prevLogIndex, prevLogTerm, logEntries, leaderCommitIndex):
        self.__term = term
        self.__leaderId = leaderId
        self.__prevLogIndex = prevLogIndex
        self.__prevLogTerm = prevLogTerm
        self.__logEntries = logEntries
        self.__leaderCommitIndex = leaderCommitIndex


class RequestVoteRPC(object):
    def __init__(self, term, candidateId, lastLogIndex, lastLogTerm):
        self.__term = term
        self.__candidateId = candidateId
        self.__lastLogIndex = lastLogIndex
        self.__lastLogTerm = lastLogTerm


class RequestVoteRPCResponse(object):
    def __init__(self, term, voteGranted):
        self.__term = term
        self.__voteGranted = voteGranted


class RaftLogEntry(object):
    def __init__(self, index, term, data):
        self.__index = index
        self.__term = term
        self.__data = data


class RaftLog(object):
    def __init__(self):
        self.__logEntries = []
        self.__commitIndex = 0
        self.__lastApplied = 0


class RaftState(object):
    def __init__(self):
        self.__currentTerm = 0 # latest term server has seen, set to 0 on boot
        self.__votedFor = None # The nodename the server voted for in the current term
        self.__log = RaftLog() # The log

        # For leader
        self.__nextIndex = {} # map of server -> next index, initialized to leader last log index + 1
        self.__matchIndex = {} # map of server -> highest replicated log entry index


class RaftServer(object):
    def __init__(self):
        pass
