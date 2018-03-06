import random
from threading import Thread
from time import sleep

from helper import helper
from rpc.messages import MessageType, AppendEntriesResponse, RequestVoteResponse, RequestVote, AppendEntries, \
    ClientData, ClientDataResponse

import logging

from scale_raft_config import ScaleRaftConfig

logger = logging.getLogger(__name__)


class BaseState(object):
    def __init__(self, server, current_term, voted_for, log, current_leader_id):
        # persistent
        self.currentTerm = current_term  # latest term server has seen, set to 0 on boot
        self.votedFor = voted_for  # The nodename the server voted for in the current term
        self.log = log  # The log

        self._currentLeaderId = current_leader_id
        # runtime only
        self.server = server

    @property
    def currentLeaderId(self):
        return self._currentLeaderId

    @currentLeaderId.setter
    def currentLeaderId(self, val):
        logger.info("Old Leader: {}, new leader: {}".format(self._currentLeaderId, val))
        self._currentLeaderId = val

    def switch_to(self, state_type):
        return state_type(self.server, self.currentTerm, self.votedFor, self.log, self.currentLeaderId)

    def handle(self, obj):
        if obj is None:
            return False, None  # stop processing

        if isinstance(obj, ClientData):
            if not isinstance(self.server.state, Leader):
                logger.info("Received client data. Forwarding client to leader: {}".format(self.currentLeaderId))
                return False, ClientDataResponse(False, self.currentLeaderId)
            return True, None  # continue processing

        if self.currentTerm < obj.term:
            self.currentTerm = obj.term
            if not isinstance(self.server.state, Follower):
                self.server.state = self.server.state.switch_to(Follower)
                return False, self.server.state.handle(obj)

        if obj.message_type == MessageType.REQUEST_VOTE:
            if obj.term < self.currentTerm:
                return False, RequestVoteResponse(self.currentTerm, False)

            if (self.votedFor is None or self.votedFor == obj.candidateId) \
                    and obj.term >= self.currentTerm \
                    and obj.lastLogIndex >= self.log.lastAppliedIndex:
                logger.info("Voting for {}".format(obj.candidateId))
                self.currentLeaderId = None
                return False, RequestVoteResponse(self.currentTerm, True)

            return False, RequestVoteResponse(self.currentTerm, False)

        if obj.message_type == MessageType.CLIENT_DATA:
            if isinstance(self.server.state, Leader):
                return True, None
            else:
                return False, ClientDataResponse(False, self.currentLeaderId)

        return True, None  # continue processing


class Leader(BaseState):
    def __init__(self, server, current_term, voted_for, log, current_leader_id):
        BaseState.__init__(self, server, current_term, voted_for, log, current_leader_id)
        # map of server -> next index, initialized to leader last log index + 1
        self.nextIndex = {hostname: self.log.lastAppliedIndex for hostname in server.peers}
        # map of server -> highest replicated log entry index
        self.matchIndex = {hostname: 0 for hostname in server.peers}
        self._heartbeat_thread = Thread(target=self.broadcast_heartbeat)
        self.log_entry_send_queue = []
        self.currentLeaderId = self.server.hostname
        self._heartbeat_thread.start()

    def broadcast_heartbeat(self):
        while isinstance(self.server.state, Leader) and not self.server.shutdown:
            sleep(0.01)  # sleep 10 millis before sending next heartbeat
            obj = AppendEntries(self.currentTerm, self.server.hostname, self.log.lastAppliedIndex,
                                self.log.lastLogTerm, self.log.commitIndex, self.log_entry_send_queue)
            self.server.broadcast(obj)
            self.log_entry_send_queue = []

    def handle(self, obj):
        (cont, resp) = BaseState.handle(self, obj)
        if not cont:
            return resp

        if obj.message_type == MessageType.CLIENT_DATA:
            logger.info("Received client data.")
            le = self.log.append_client_data(obj.data)
            if len(self.server.peers) == 0:
                # commit instantly
                self.log.commitIndex = le.index
            else:
                # Queue for sending
                self.log_entry_send_queue.append(le)
            # FIXME add timeout to avoid looping forever
            #while self.log.commitIndex != le.index:
            #    pass
            return ClientDataResponse(True, self.currentLeaderId)


class Follower(BaseState):
    def __init__(self, server, current_term, voted_for, log, current_leader_id):
        BaseState.__init__(self, server, current_term, voted_for, log, current_leader_id)
        self.leaderId = 0

    def handle(self, obj):
        (cont, resp) = BaseState.handle(self, obj)
        if not cont:
            return resp

        BaseState.handle(self, obj)

        if obj.message_type == MessageType.APPEND_ENTRIES:
            self.leaderId = obj.leaderId

            # Local log has higher term/is more current
            if obj.term < self.currentTerm:
                return AppendEntriesResponse(self.currentTerm, False)

            # The local log has no entry at prevLogIndex, prevLogTerm
            if self.log.exists(obj.prevLogIndex, obj.prevLogTerm):
                return AppendEntriesResponse(self.currentTerm, False)

            # Append the new entries
            index_of_last_new_entry = self.log.append_entries(obj.logEntries)

            # set the current commit index to either the leader commit index or the index of the last new entry
            # (leader may have sent only older entries)
            if obj.leaderCommitIndex > self.log.commitIndex:
                self.log.commitIndex = min(obj.leaderCommitIndex, index_of_last_new_entry)
            return AppendEntriesResponse(self.currentTerm, True)

        elif obj.message_type == MessageType.REQUEST_VOTE_RESPONSE:
            return None

        elif obj.message_type == MessageType.APPEND_ENTRIES_RESPONSE:
            return None

        logger.error("Received unexpected message (type=%d) for state Follower" % obj.message_type)


class Candidate(BaseState):
    def __init__(self, server, current_term, voted_for, log, current_leader_id):
        BaseState.__init__(self, server, current_term, voted_for, log, current_leader_id)
        # start election
        self.currentTerm += 1
        self.votedFor = server.hostname
        self.vote_counter = 1
        if len(server.peers) > 0:
            request_vote_rpc = RequestVote(self.currentTerm, server.hostname, self.log.lastAppliedIndex,
                                           self.log.lastLogTerm)
            random.seed(helper.get_current_time_millis())
            sleep(random.randint(
                ScaleRaftConfig().ELECTION_TIMEOUT_IN_MILLIS_MIN,
                ScaleRaftConfig().ELECTION_TIMEOUT_IN_MILLIS_MAX) / 1000.0)
            self.server.broadcast(request_vote_rpc)

    def handle(self, obj):
        (cont, resp) = BaseState.handle(self, obj)
        if not cont:
            return resp

        BaseState.handle(self, obj)
        if obj.message_type == MessageType.REQUEST_VOTE_RESPONSE:
            if obj.voteGranted is True \
                    and obj.term >= self.currentTerm:
                self.vote_counter += 1
                if self.vote_counter > (len(self.server.peers) / 2):
                    self.server.state = self.server.state.switch_to(Leader)
            return

        if obj.message_type == MessageType.APPEND_ENTRIES:
            if obj.term >= self.currentTerm:
                self.currentLeaderId = obj.leaderId
                self.server.state = self.server.state.switch_to(Follower)
                self.server.state.handle(obj)
            return

        logger.error("Received unexpected message (type=%d) for state Candidate" % obj.message_type)

