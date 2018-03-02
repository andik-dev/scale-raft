
class Log(object):
    def __init__(self):
        self.logEntries = []
        self.commitIndex = 0
        self.lastApplied = 0

    def exists(self, index, term):
        if len(self.logEntries) == 0:
            return False
        # TODO avoid looping by querying index directly
        for entry in self.logEntries:
            if entry.__index == index and entry.__term != term:
                return False
            elif entry.__index == index and entry.__term == term:
                return True
        return False

    def append_entries(self, *log_entries):
        self.logEntries.append(log_entries)



