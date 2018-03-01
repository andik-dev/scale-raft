from persistence import log
from rpc.rpc import MessageType, AppendEntriesRPCResponse


class BaseState(object):
    def __init__(self, server):
        # persistent
        self.__currentTerm = 0  # latest term server has seen, set to 0 on boot
        self.__votedFor = None  # The nodename the server voted for in the current term
        self.__log = log.Log()  # The log

        # can be volatile
        self.__commitIndex = 0
        self.__lastApplied = 0

        # runtime only
        self.__server = server

    def handle_msg(self, message):
        return None


class Leader(BaseState):
    def __init__(self, server):
        BaseState.__init__(self, server)
        self.__nextIndex = {}  # map of server -> next index, initialized to leader last log index + 1
        self.__matchIndex = {}  # map of server -> highest replicated log entry index
        pass


class Follower(BaseState):
    def __init__(self, server):
        BaseState.__init__(self, server)
        self.__leaderId = 0
        pass

    def handle_msg(self, msg):
        BaseState.handle_msg(self, msg)
        resp = None

        if msg.message_type == MessageType.APPEND_ENTRIES:
            self.__leaderId = msg.leaderId
            if msg.term < self.__currentTerm:
                resp = AppendEntriesRPCResponse(self.__currentTerm, False)
            if self.__log.exists_entry(msg.prevLogIndex, msg.prevLogTerm):
                resp = AppendEntriesRPCResponse(self.__currentTerm, False)

            index_of_last_new_entry = self.__log.append_entries(msg.entries)
            if msg.leaderCommit > self.__commitIndex:
                self.__commitIndex = min(msg.leaderCommit, index_of_last_new_entry)

        elif msg.message_type == MessageType.APPEND_ENTRIES_RESPONSE:
            pass

        elif msg.message_type == MessageType.REQUEST_VOTE:
            pass

        elif msg.message_type == MessageType.REQUEST_VOTE_RESPONSE:
            pass

        if resp is not None:
            self.__server.send(resp)


class Candidate(BaseState):
    def __init__(self, server):
        super(Candidate, self).__init__(server)
