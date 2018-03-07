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
        logger.info("{}: Old Leader: {}, new leader: {}".format(self.server.hostname, self._currentLeaderId, val))
        self._currentLeaderId = val

    def switch_to(self, state_type):
        return state_type(self.server, self.currentTerm, self.votedFor, self.log, self.currentLeaderId)

    def handle(self, obj):
        if obj is None:
            return False, None  # stop processing

        if isinstance(obj, ClientData):
            if not isinstance(self.server.state, Leader):
                logger.info("{}: Received client data but not a leader. Forwarding client to leader: {}".format(self.server.hostname, self.currentLeaderId))
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
                logger.info("{}: Voting for {}".format(self.server.hostname, obj.candidateId))
                self.currentLeaderId = None
                self.votedFor = obj.candidateId
                if not isinstance(self.server.state, Follower):
                    self.server.state = self.server.state.switch_to(Follower)
                logger.info("{}: Voted for {}".format(self.server.hostname, obj.candidateId))
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
            obj = AppendEntries(self.currentTerm, self.server.hostname, self.log.lastAppliedIndex,
                                self.log.lastLogTerm, self.log.commitIndex, self.log_entry_send_queue)
            self.server.broadcast(obj)
            self.log_entry_send_queue = []
            sleep(0.01)  # sleep before sending next heartbeat

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

    def handle(self, obj):
        (cont, resp) = BaseState.handle(self, obj)
        if not cont:
            return resp

        if obj.message_type == MessageType.APPEND_ENTRIES:
            # Local log has higher term/is more current
            if obj.term < self.currentTerm:
                return AppendEntriesResponse(self.currentTerm, False)

            # The local log has no entry at prevLogIndex, prevLogTerm
            if self.log.exists(obj.prevLogIndex, obj.prevLogTerm):
                return AppendEntriesResponse(self.currentTerm, False)

            if self.currentLeaderId != obj.leaderId:
                logger.info("{}: New leader: {}".format(self.server.hostname, obj.leaderId))
                self.currentLeaderId = obj.leaderId

            # Append the new entries
            index_of_last_new_entry = self.log.append_entries(obj.logEntries)

            # set the current commit index to either the leader commit index or the index of the last new entry
            # (leader may have sent only older entries)
            if obj.leaderCommitIndex > self.log.commitIndex:
                self.log.commitIndex = min(obj.leaderCommitIndex, index_of_last_new_entry)
            return AppendEntriesResponse(self.currentTerm, True)

        logger.error("Received unexpected message (type=%d) for state Follower" % obj.message_type)


class Candidate(BaseState):
    def __init__(self, server, current_term, voted_for, log, current_leader_id):
        BaseState.__init__(self, server, current_term, voted_for, log, current_leader_id)
        # start election
        self._start_election_thread = Thread(target=self._start_election)
        self.votedFor = None
        self.voteCounter = 0
        self._start_election_thread.start()

    def _start_election(self):
        # Sleep a random time before starting a vote
        random.seed(helper.get_current_time_nanos())
        sleep_seconds = random.randint(
            ScaleRaftConfig().ELECTION_TIMEOUT_IN_MILLIS_MIN,
            ScaleRaftConfig().ELECTION_TIMEOUT_IN_MILLIS_MAX) / 1000.0
        logger.info("{}: Sleeping {} seconds before starting a vote".format(self.server.hostname, sleep_seconds))
        sleep(sleep_seconds)

        # check if votedFor is still None
        if self.server.state.votedFor is None:
            logger.info("{}: Starting vote...".format(self.server.hostname))
            self.currentTerm += 1
            self.votedFor = self.server.hostname
            self.voteCounter = 1
            if len(self.server.peers) > 0:
                request_vote_rpc = RequestVote(self.currentTerm, self.server.hostname, self.log.lastAppliedIndex,
                                               self.log.lastLogTerm)
                self.server.broadcast(request_vote_rpc)

    def handle(self, obj):
        (cont, resp) = BaseState.handle(self, obj)
        if not cont:
            return resp

        if obj.message_type == MessageType.REQUEST_VOTE_RESPONSE:
            if obj.voteGranted is True and obj.term == self.currentTerm and self.votedFor == self.server.hostname:
                self.voteCounter += 1
                logger.info("{}: Received {} votes".format(self.server.hostname, self.voteCounter))
                if self.voteCounter > (len(self.server.peers) / 2) and not isinstance(self.server.state, Leader):
                    logger.info("{}: Received {} votes. Switching to Leader".format(self.server.hostname, self.voteCounter))
                    self.server.state = self.server.state.switch_to(Leader)
                    return
            return

        if obj.message_type == MessageType.APPEND_ENTRIES:
            if obj.term >= self.currentTerm:
                self.server.state = self.server.state.switch_to(Follower)
                return self.server.state.handle(obj)
            return AppendEntriesResponse(False, self.currentTerm)

        logger.error("Received unexpected message (type=%d) for state Candidate" % obj.message_type)

