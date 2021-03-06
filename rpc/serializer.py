import base64

from rpc.messages import BaseMessage, RequestVote, RequestVoteResponse, AppendEntriesResponse, \
    AppendEntries, LogEntry, MessageType, ClientData, ClientDataResponse


class RaftSerializer(object):
    FIELD_SEPARATOR = ":"

    def __init__(self):
        pass

    def deserialize(self, string):
        # version:message_type:PAYLOAD
        (version, message_type, payload) = string.split(RaftSerializer.FIELD_SEPARATOR, 2)
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
        elif message_type == MessageType.CLIENT_DATA:
            obj = ClientDataSerializer.deserialize_from_payload(payload)
        elif message_type == MessageType.CLIENT_DATA_RESPONSE:
            obj = ClientDataResponseSerializer.deserialize_from_payload(payload)

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
        elif obj.message_type == MessageType.CLIENT_DATA:
            string = ClientDataSerializer.serialize_to_string(obj)
        elif obj.message_type == MessageType.CLIENT_DATA_RESPONSE:
            string = ClientDataResponseSerializer.serialize_to_string(obj)

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
        return RaftSerializer.FIELD_SEPARATOR.join(res)


class InvalidMessageVersionException(Exception):
    def __init__(self, expected, actual):
        self.expected = expected
        self.actual = actual

    def __str__(self):
        return "Invalid message version: Expected {}, received {}".format(self.expected, self.actual)


class RequestVoteSerializer(RaftSerializer):
    @staticmethod
    def serialize_to_string(obj):
        return RaftSerializer._serialize_array_to_string(obj.version, obj.message_type, obj.term, obj.candidateId,
                                                       obj.lastLogIndex, obj.lastLogTerm)

    @staticmethod
    def deserialize_from_payload(payload):
        (term, candidate_id, last_log_index, last_log_term) = payload.split(RaftSerializer.FIELD_SEPARATOR, 4)
        return RequestVote(term, candidate_id, last_log_index, last_log_term)


class RequestVoteResponseSerializer(RaftSerializer):
    @staticmethod
    def serialize_to_string(obj):
        return RaftSerializer._serialize_array_to_string(obj.version, obj.message_type, obj.term, obj.voteGranted)

    @staticmethod
    def deserialize_from_payload(payload):
        (term, vote_granted) = payload.split(RaftSerializer.FIELD_SEPARATOR, 1)
        if vote_granted == "True":
            vote_granted = True
        else:
            vote_granted = False
        return RequestVoteResponse(term, vote_granted)


class AppendEntriesResponseSerializer(RaftSerializer):
    @staticmethod
    def serialize_to_string(obj):
        return RaftSerializer._serialize_array_to_string(obj.version, obj.message_type, obj.term,
                                                       obj.success)

    @staticmethod
    def deserialize_from_payload(payload):
        (term, success) = payload.split(RaftSerializer.FIELD_SEPARATOR, 2)
        return AppendEntriesResponse(term, success)


class AppendEntriesSerializer(RaftSerializer):
    LOG_ENTRY_FIELD_SEPARATOR = ","
    LOG_ENTRY_SEPARATOR = ";"

    @staticmethod
    def serialize_to_string(obj):
            # encode log entries
            encoded_log_entries = []
            for le in obj.logEntries:
                encoded_le = AppendEntriesSerializer.LOG_ENTRY_FIELD_SEPARATOR\
                    .join([str(le.index), str(le.term), base64.standard_b64encode(str(le.data))])
                encoded_log_entries.append(encoded_le)

            return RaftSerializer._serialize_array_to_string(obj.version, obj.message_type, obj.term,
                                                                  obj.leaderId, obj.prevLogIndex, obj.prevLogTerm,
                                                                  obj.leaderCommitIndex,
                                                                  AppendEntriesSerializer.LOG_ENTRY_SEPARATOR
                                                                  .join(encoded_log_entries))

    @staticmethod
    def deserialize_from_payload(payload):
        (term, leader_id, prev_log_index, prev_log_term, leader_commit_index, encoded_log_entries) = payload.split(
            RaftSerializer.FIELD_SEPARATOR, 5)

        # decode log entries
        log_entries = []
        if len(encoded_log_entries) != 0:
            for encoded_le in encoded_log_entries.split(AppendEntriesSerializer.LOG_ENTRY_SEPARATOR):
                (index, term, data) = encoded_le.split(AppendEntriesSerializer.LOG_ENTRY_FIELD_SEPARATOR)
                data = base64.standard_b64decode(data)
                log_entries.append(LogEntry(index, term, data))

        return AppendEntries(term, leader_id, prev_log_index, prev_log_term, leader_commit_index,
                             log_entries)


class ClientDataSerializer(RaftSerializer):
    @staticmethod
    def serialize_to_string(obj):
        return RaftSerializer._serialize_array_to_string(obj.version, obj.message_type, obj.data)

    @staticmethod
    def deserialize_from_payload(payload):
        return ClientData(payload)


class ClientDataResponseSerializer(RaftSerializer):
    @staticmethod
    def serialize_to_string(obj):
        return RaftSerializer._serialize_array_to_string(obj.version, obj.message_type, obj.success, obj.leaderId)

    @staticmethod
    def deserialize_from_payload(payload):
        (success, leaderId) = payload.split(RaftSerializer.FIELD_SEPARATOR, 2)
        if success == "True":
            success = True
        else:
            success = False
        return ClientDataResponse(success, leaderId)
