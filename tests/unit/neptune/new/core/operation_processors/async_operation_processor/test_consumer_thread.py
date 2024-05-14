#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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

import unittest
from unittest.mock import (
    MagicMock,
    Mock,
    patch,
)

from neptune.core.operation_processors.async_operation_processor.consumer_thread import ConsumerThread


class TestConsumerThread(unittest.TestCase):
    @patch(
        "neptune.core.operation_processors.async_operation_processor.consumer_thread.Daemon.run",
        new=Mock(side_effect=Exception),
    )
    def test_run_raises_exception_from_daemon_but_notifies_first(self):
        # given
        customer_thread = ConsumerThread(
            sleep_time=30,
            processing_resources=Mock(),
        )

        customer_thread._processing_resources.waiting_cond = MagicMock()

        # then
        with self.assertRaises(Exception):
            customer_thread.run()

        customer_thread._processing_resources.waiting_cond.notify_all.assert_called_once()

    @patch("neptune.core.operation_processors.async_operation_processor.consumer_thread.signal_batch_started")
    def test_work(self, signal_batch_started):
        # given
        customer_thread = ConsumerThread(
            sleep_time=30,
            processing_resources=Mock(),
        )
        customer_thread.process_batch = Mock()
        customer_thread._processing_resources.disk_queue = MagicMock()
        customer_thread._processing_resources.batch_size = 100
        customer_thread._processing_resources.disk_queue.get_batch.side_effect = [[MagicMock()], []]
        customer_thread._processing_resources.waiting_cond = MagicMock()

        # when
        customer_thread.work()

        # then
        customer_thread._processing_resources.disk_queue.flush.assert_called_once()
        customer_thread._processing_resources.disk_queue.get_batch.assert_has_calls([((100,),), ((100,),)])
        customer_thread.process_batch.assert_called_once()
        signal_batch_started.assert_called_once()

    @patch("neptune.core.operation_processors.async_operation_processor.consumer_thread.signal_batch_processed")
    @patch("neptune.core.operation_processors.async_operation_processor.consumer_thread.signal_batch_lag")
    def test_process_batch_occurred_at_not_supplied(self, signal_batch_lag, signal_batch_processed):
        # given
        customer_thread = ConsumerThread(
            sleep_time=30,
            processing_resources=Mock(),
        )

        operation = MagicMock()
        customer_thread._processing_resources.waiting_cond = MagicMock()

        # when
        customer_thread.process_batch(batch=[operation], version=0)

        # then
        signal_batch_lag.assert_not_called()
        signal_batch_processed.assert_called_once_with(queue=customer_thread._processing_resources.signals_queue)
        customer_thread._processing_resources.disk_queue.ack.assert_called_once()
        customer_thread._processing_resources.waiting_cond.notify_all.assert_called_once()

    @patch("neptune.core.operation_processors.async_operation_processor.consumer_thread.signal_batch_processed")
    @patch("neptune.core.operation_processors.async_operation_processor.consumer_thread.signal_batch_lag")
    def test_process_batch_occurred_at_supplied(self, signal_batch_lag, signal_batch_processed):
        # given
        customer_thread = ConsumerThread(
            sleep_time=30,
            processing_resources=Mock(),
        )

        operation = MagicMock()
        customer_thread._processing_resources.waiting_cond = MagicMock()

        # when
        customer_thread.process_batch(batch=[operation], version=0, occurred_at=0)

        # then
        signal_batch_lag.assert_called_once()
        signal_batch_processed.assert_called_once_with(queue=customer_thread._processing_resources.signals_queue)
        customer_thread._processing_resources.disk_queue.ack.assert_called_once()
        customer_thread._processing_resources.waiting_cond.notify_all.assert_called_once()
