#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from neptune.new.attributes.constants import (
    SIGNAL_TYPE_ABORT,
    SIGNAL_TYPE_STOP,
)


class Message(object):
    def __init__(self):
        pass

    MESSAGE_TYPE = "messageType"
    MESSAGE_NEW_TYPE = "type"
    MESSAGE_BODY = "messageBody"
    MESSAGE_NEW_BODY = "body"

    @classmethod
    def from_json(cls, json_value):
        message_type = json_value.get(Message.MESSAGE_TYPE) or json_value.get(Message.MESSAGE_NEW_TYPE)
        message_body = json_value.get(Message.MESSAGE_BODY) or json_value.get(Message.MESSAGE_NEW_BODY)

        if message_type == SIGNAL_TYPE_STOP:
            message_type = MessageType.STOP
        elif message_type == SIGNAL_TYPE_ABORT:
            message_type = MessageType.ABORT

        if message_type in MessageClassRegistry.MESSAGE_CLASSES:
            return MessageClassRegistry.MESSAGE_CLASSES[message_type].from_json(message_body)
        else:
            raise ValueError("Unknown message type '{}'!".format(message_type))

    @classmethod
    def get_type(cls):
        raise NotImplementedError()

    def body_to_json(self):
        raise NotImplementedError()


class AbortMessage(Message):
    @classmethod
    def get_type(cls):
        return MessageType.ABORT

    @classmethod
    def from_json(cls, json_value):
        return AbortMessage()

    def body_to_json(self):
        return None


class StopMessage(Message):
    @classmethod
    def get_type(cls):
        return MessageType.STOP

    @classmethod
    def from_json(cls, json_value):
        return StopMessage()

    def body_to_json(self):
        return None


class ActionInvocationMessage(Message):
    _ACTION_ID_JSON_KEY = "actionId"
    _ACTION_INVOCATION_ID_JSON_KEY = "actionInvocationId"
    _ARGUMENT_JSON_KEY = "argument"

    def __init__(self, action_id, action_invocation_id, argument):
        super(ActionInvocationMessage, self).__init__()
        self.action_id = action_id
        self.action_invocation_id = action_invocation_id
        self.argument = argument

    @classmethod
    def get_type(cls):
        return MessageType.ACTION_INVOCATION

    @classmethod
    def from_json(cls, json_value):
        field_names = [
            cls._ACTION_ID_JSON_KEY,
            cls._ACTION_INVOCATION_ID_JSON_KEY,
            cls._ARGUMENT_JSON_KEY,
        ]
        return ActionInvocationMessage(*[json_value[field] for field in field_names])

    def body_to_json(self):
        return {
            self._ACTION_ID_JSON_KEY: self.action_id,
            self._ACTION_INVOCATION_ID_JSON_KEY: self.action_invocation_id,
            self._ARGUMENT_JSON_KEY: self.argument,
        }


class MessageType(object):
    NEW_CHANNEL_VALUES = "NewChannelValues"
    ABORT = "Abort"
    STOP = "Stop"
    ACTION_INVOCATION = "InvokeAction"


class MessageClassRegistry(object):
    def __init__(self):
        pass

    MESSAGE_CLASSES = dict([(cls.get_type(), cls) for cls in Message.__subclasses__()])
