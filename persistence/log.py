class Log(object):
    def __init__(self):
        self.__logEntries = []
        self.__commitIndex = 0
        self.__lastApplied = 0

    def exists_entry(self, index, term):
        if len(self.__logEntries) == 0:
            return False
        # TODO avoid looping by querying index directly
        for entry in self.__logEntries:
            if entry.__index == index and entry.__term != term:
                return False
            elif entry.__index == index and entry.__term == term:
                return True
        return False

    def append_entries(self, *log_entries):
        self.__logEntries.append(log_entries)


class LogEntry(object):
    def __init__(self, index, term, data):
        self.__index = index
        self.__term = term
        self.__data = data
