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
import logging

_logger = logging.getLogger(__name__)


class MessageType(object):
    CHECKPOINT_CREATED = "CHECKPOINT_CREATED"


def send_checkpoint_created(notebook_id, notebook_path, checkpoint_id):
    """Send checkpoint created message.

    Args:
        notebook_id (:obj:`str`): The notebook's id.
        notebook_path (:obj:`str`): The notebook's path.
        checkpoint_id (:obj:`str`): The checkpoint's path.


    Raises:
        `ImportError`: If ipykernel is not available.
    """
    neptune_comm = _get_comm()
    neptune_comm.send(data=dict(
        message_type=MessageType.CHECKPOINT_CREATED,
        data=dict(checkpoint_id=checkpoint_id,
                  notebook_id=notebook_id,
                  notebook_path=notebook_path)))


def _get_comm():
    # pylint: disable=import-error
    from ipykernel.comm import Comm
    return Comm(target_name='neptune_comm')
