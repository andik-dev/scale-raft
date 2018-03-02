import base64

from rpc.messages import BaseMessage, RequestVote, RequestVoteResponse, AppendEntriesResponse, \
    AppendEntries, LogEntry, MessageType


class ScaleRaftSerializer(object):
    FIELD_SEPARATOR = ":"

    def __init__(self):
        pass

    def deserialize(self, string):
        # version:message_type:PAYLOAD
        (version, message_type, payload) = string.split(ScaleRaftSerializer.FIELD_SEPARATOR, 2)
        version = int(version)
        message_type = int(message_type)
        if version != BaseMessage.VERSION:
            raise InvalidMessageVersionException(BaseMessage.VERSION, version)

        obj = None

        if message_type == MessageType.REQUEST_VOTE:
            obj = RequestVoteSerializer.deserialize_from_payload(payload)
        elif message_type == MessageType.REQUEST_VOTE_RESPONSE:
            obj = RequestVoteResponseSerializer.deserialize_from_payload(payload)
        elif message_type == MessageType.APPEND_ENTRIES:
            obj = AppendEntriesSerializer.deserialize_from_payload(payload)
        elif message_type == MessageType.APPEND_ENTRIES_RESPONSE:
            obj = AppendEntriesResponseSerializer.deserialize_from_payload(payload)

        assert obj is not None, "Unknown message type: %d" % message_type
        return obj

    def serialize(self, obj):
        string = None

        if obj.message_type == MessageType.REQUEST_VOTE:
            string = RequestVoteSerializer.serialize_to_string(obj)
        elif obj.message_type == MessageType.REQUEST_VOTE_RESPONSE:
            string = RequestVoteResponseSerializer.serialize_to_string(obj)
        elif obj.message_type == MessageType.APPEND_ENTRIES:
            string = AppendEntriesSerializer.serialize_to_string(obj)
        elif obj.message_type == MessageType.APPEND_ENTRIES_RESPONSE:
            string = AppendEntriesResponseSerializer.serialize_to_string(obj)

        assert string is not None, "Unknown message type: %d" % obj.message_type
        return string

    @staticmethod
    def serialize_to_string(obj):
        pass

    @staticmethod
    def deserialize_from_payload(payload):
        pass

    @staticmethod
    def _serialize_array_to_string(*data):
        res = [str(d) for d in data]
        return ScaleRaftSerializer.FIELD_SEPARATOR.join(res)


class InvalidMessageVersionException(Exception):
    def __init__(self, expected, actual):
        self.expected = expected
        self.actual = actual

    def __str__(self):
        return "Invalid message version: Expected {}, received {}".format(self.expected, self.actual)


class RequestVoteSerializer(ScaleRaftSerializer):
    @staticmethod
    def serialize_to_string(obj):
        return ScaleRaftSerializer._serialize_array_to_string(obj.version, obj.message_type, obj.term, obj.candidateId,
                                                       obj.lastLogIndex, obj.lastLogTerm)

    @staticmethod
    def deserialize_from_payload(payload):
        (term, candidate_id, last_log_index, last_log_term) = payload.split(ScaleRaftSerializer.FIELD_SEPARATOR, 4)
        return RequestVote(term, candidate_id, last_log_index, last_log_term)


class RequestVoteResponseSerializer(ScaleRaftSerializer):
    @staticmethod
    def serialize_to_string(obj):
        return ScaleRaftSerializer._serialize_array_to_string(obj.version, obj.message_type, obj.term, obj.voteGranted)

    @staticmethod
    def deserialize_from_payload(payload):
        (term, vote_granted) = payload.split(ScaleRaftSerializer.FIELD_SEPARATOR, 1)
        return RequestVoteResponse(term, vote_granted)


class AppendEntriesResponseSerializer(ScaleRaftSerializer):
    @staticmethod
    def serialize_to_string(obj):
        return ScaleRaftSerializer._serialize_array_to_string(obj.version, obj.message_type, obj.term,
                                                       obj.success)

    @staticmethod
    def deserialize_from_payload(payload):
        (term, success) = payload.split(ScaleRaftSerializer.FIELD_SEPARATOR, 2)
        return AppendEntriesResponse(term, success)

class AppendEntriesSerializer(ScaleRaftSerializer):
    LOG_ENTRY_FIELD_SEPARATOR = ","
    LOG_ENTRY_SEPARATOR = ";"

    @staticmethod
    def serialize_to_string(obj):
            # encode log entries
            encoded_log_entries = []
            for le in obj.logEntries:
                encoded_le = AppendEntriesSerializer.LOG_ENTRY_FIELD_SEPARATOR.join([str(le.index), str(le.term),
                                                                                     base64.standard_b64encode(str(le.data))])
                encoded_log_entries.append(encoded_le)

            return ScaleRaftSerializer._serialize_array_to_string(obj.version, obj.message_type, obj.term,
                                                                  obj.leaderId, obj.prevLogIndex, obj.prevLogTerm,
                                                                  obj.leaderCommitIndex,
                                                                  AppendEntriesSerializer.LOG_ENTRY_SEPARATOR
                                                                  .join(encoded_log_entries))

    @staticmethod
    def deserialize_from_payload(payload):
        (term, leader_id, prev_log_index, prev_log_term, leader_commit_index, encoded_log_entries) = payload.split(
            ScaleRaftSerializer.FIELD_SEPARATOR, 5)

        # decode log entries
        log_entries = []
        for encoded_le in encoded_log_entries.split(AppendEntriesSerializer.LOG_ENTRY_SEPARATOR):
            (index, term, data) = encoded_le.split(AppendEntriesSerializer.LOG_ENTRY_FIELD_SEPARATOR)
            data = base64.standard_b64decode(data)
            log_entries.append(LogEntry(index, term, data))

        return AppendEntries(term, leader_id, prev_log_index, prev_log_term, leader_commit_index,
                             log_entries)
