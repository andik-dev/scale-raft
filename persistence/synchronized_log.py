import threading

from helper.helper import synchronized
from rpc.messages import LogEntry

import logging

logger = logging.getLogger(__name__)

class SynchronizedLog(object):
    def __init__(self):
        self.logEntries = {}
        self._commitIndex = 0
        self.lastAppliedIndex = 0
        self.lastLogTerm = 0
        self.lock = threading.Lock()

    def get_lock(self):
        return self.lock

    @property
    def commitIndex(self):
        return self._commitIndex
    
    @commitIndex.setter
    # @synchronized(get_lock)
    def commitIndex(self, commit_index):
        logger.debug("Setting commit index from {} to {}".format(self._commitIndex, commit_index))
        self._commitIndex = commit_index
        if self._commitIndex > self.lastAppliedIndex:
            self.lastAppliedIndex = self.apply(self.lastAppliedIndex, self._commitIndex)

    def exists(self, index, term):
        if len(self.logEntries) == 0:
            return False
        try:
            entry = self.logEntries[index]
            if entry.term != term:
                return False
            else:
                return True
        except KeyError:
            pass
        return False

    def append_entries(self, log_entries):
        for le in log_entries:
            try:
                existing_entry = self.logEntries[le.index]
                if existing_entry.term == le.term:
                    continue
            except KeyError:
                pass
            self.logEntries[le.index] = le

    def apply(self, fromIndex, toIndex):
        # TODO apply LogEntries to state machine
        return toIndex

    def append_client_data(self, data):
        self.lastAppliedIndex += 1
        le = LogEntry(self.lastAppliedIndex, self.lastLogTerm, data)
        self.logEntries[self.lastAppliedIndex] = le
        return le
