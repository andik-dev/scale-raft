from persistence import log
from rpc.messages import MessageType, AppendEntriesResponse


class BaseState(object):
    def __init__(self, server):
        # persistent
        self.currentTerm = 0  # latest term server has seen, set to 0 on boot
        self.votedFor = None  # The nodename the server voted for in the current term
        self.log = log.Log()  # The log

        # can be volatile
        self.commitIndex = 0
        self.lastApplied = 0

        # runtime only
        self.server = server

    def handle(self, message):
        pass


class Leader(BaseState):
    def __init__(self, server):
        BaseState.__init__(self, server)
        self.nextIndex = {}  # map of server -> next index, initialized to leader last log index + 1
        self.matchIndex = {}  # map of server -> highest replicated log entry index
        pass


class Follower(BaseState):
    def __init__(self, server):
        BaseState.__init__(self, server)
        self.leaderId = 0
        pass

    def handle(self, msg):
        BaseState.handle(self, msg)
        resp = None

        if msg.message_type == MessageType.APPEND_ENTRIES:
            self.leaderId = msg.leaderId
            if msg.term < self.currentTerm:
                resp = AppendEntriesResponse(self.currentTerm, False)
            if self.log.exists(msg.prevLogIndex, msg.prevLogTerm):
                resp = AppendEntriesResponse(self.currentTerm, False)

            index_of_last_new_entry = self.log.append_entries(msg.logEntries)
            if msg.leaderCommitIndex > self.commitIndex:
                self.commitIndex = min(msg.leaderCommitIndex, index_of_last_new_entry)
            resp = AppendEntriesResponse(self.currentTerm, True)

        elif msg.message_type == MessageType.APPEND_ENTRIES_RESPONSE:
            pass

        elif msg.message_type == MessageType.REQUEST_VOTE:
            pass

        elif msg.message_type == MessageType.REQUEST_VOTE_RESPONSE:
            pass

        return resp


class Candidate(BaseState):
    def __init__(self, server):
        super(Candidate, self).__init__(server)
