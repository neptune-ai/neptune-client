#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
from queue import Queue

from mock import (
    MagicMock,
    patch,
)

from neptune.internal.signals_processing.signals_processor import SignalsProcessor
from neptune.internal.signals_processing.utils import (
    signal_batch_processed,
    signal_batch_started,
)


def test__no_progress__no_signal():
    # given
    container = MagicMock()
    async_no_progress_callback = MagicMock()

    # and
    queue = Queue()

    # and
    processor = SignalsProcessor(
        period=10,
        container=container,
        queue=queue,
        async_lag_threshold=1.0,
        async_no_progress_threshold=1.0,
        async_no_progress_callback=async_no_progress_callback,
        callbacks_interval=5,
    )

    # when
    processor.work()

    # then
    async_no_progress_callback.assert_not_called()


@patch("neptune.internal.signals_processing.signals_processor.monotonic")
def test__no_progress__proper_execution_of_batch(monotonic):
    # given
    container = MagicMock()
    async_no_progress_callback = MagicMock()

    # and
    monotonic.side_effect = [
        5.0,
    ]

    # and
    queue = Queue()
    # First proper batch
    signal_batch_started(queue=queue, occured_at=0.0)
    signal_batch_processed(queue=queue, occured_at=3.0)

    # and
    processor = SignalsProcessor(
        period=10,
        container=container,
        queue=queue,
        async_lag_threshold=1.0,
        async_no_progress_threshold=5.0,
        async_no_progress_callback=async_no_progress_callback,
        callbacks_interval=5,
    )

    # when
    processor.work()

    # then
    assert len(monotonic.mock_calls) == 1
    async_no_progress_callback.assert_not_called()


@patch("neptune.internal.signals_processing.signals_processor.monotonic")
def test__no_progress__too_long_batch_execution(monotonic):
    # given
    container = MagicMock()
    async_no_progress_callback = MagicMock()

    # and
    monotonic.side_effect = [11.0, 11.01]

    # and
    queue = Queue()
    # First too long batch
    signal_batch_started(queue=queue, occured_at=0.0)
    signal_batch_processed(queue=queue, occured_at=10.0)

    # and
    processor = SignalsProcessor(
        period=10,
        container=container,
        queue=queue,
        async_lag_threshold=1.0,
        async_no_progress_threshold=5.0,
        async_no_progress_callback=async_no_progress_callback,
        callbacks_interval=5,
    )

    # when
    processor.work()

    # then
    assert len(monotonic.mock_calls) == 2
    async_no_progress_callback.assert_called_once_with(container)


@patch("neptune.internal.signals_processing.signals_processor.monotonic")
def test__no_progress__proper_then_too_long(monotonic):
    # given
    container = MagicMock()
    async_no_progress_callback = MagicMock()

    # and
    monotonic.side_effect = [16.0, 16.01]

    # and
    queue = Queue()
    # First proper batch
    signal_batch_started(queue=queue, occured_at=0.0)
    signal_batch_processed(queue=queue, occured_at=4.0)
    # Second too long batch
    signal_batch_started(queue=queue, occured_at=5.0)
    signal_batch_processed(queue=queue, occured_at=15.0)

    # and
    processor = SignalsProcessor(
        period=10,
        container=container,
        queue=queue,
        async_lag_threshold=1.0,
        async_no_progress_threshold=5.0,
        async_no_progress_callback=async_no_progress_callback,
        callbacks_interval=5,
    )

    # when
    processor.work()

    # then
    assert len(monotonic.mock_calls) == 2
    async_no_progress_callback.assert_called_once_with(container)


@patch("neptune.internal.signals_processing.signals_processor.monotonic")
def test__no_progress__proper_then_non_ended(monotonic):
    # given
    container = MagicMock()
    async_no_progress_callback = MagicMock()

    # and
    monotonic.side_effect = [16.0, 16.01]

    # and
    queue = Queue()
    # First proper batch
    signal_batch_started(queue=queue, occured_at=0.0)
    signal_batch_processed(queue=queue, occured_at=4.0)
    # Second non-ended batch
    signal_batch_started(queue=queue, occured_at=5.0)

    # and
    processor = SignalsProcessor(
        period=10,
        container=container,
        queue=queue,
        async_lag_threshold=1.0,
        async_no_progress_threshold=5.0,
        async_no_progress_callback=async_no_progress_callback,
        callbacks_interval=5,
    )

    # when
    processor.work()

    # then
    assert len(monotonic.mock_calls) == 2
    async_no_progress_callback.assert_called_once_with(container)


@patch("neptune.internal.signals_processing.signals_processor.monotonic")
def test__no_progress__too_short_time_between_callbacks(monotonic):
    # given
    container = MagicMock()
    async_no_progress_callback = MagicMock()

    # and
    monotonic.side_effect = [14.0, 14.01, 14.02]

    # and
    queue = Queue()
    # First failing batch
    signal_batch_started(queue=queue, occured_at=0.0)
    signal_batch_processed(queue=queue, occured_at=6.0)
    # Almost immediate second failing batch
    signal_batch_started(queue=queue, occured_at=7.0)
    signal_batch_processed(queue=queue, occured_at=13.0)

    # and
    processor = SignalsProcessor(
        period=10,
        container=container,
        queue=queue,
        async_lag_threshold=1.0,
        async_no_progress_threshold=5.0,
        async_no_progress_callback=async_no_progress_callback,
        callbacks_interval=5,
    )

    # when
    processor.work()

    # then
    assert len(monotonic.mock_calls) == 3
    async_no_progress_callback.assert_called_once_with(container)


@patch("neptune.internal.signals_processing.signals_processor.monotonic")
def test__no_progress__ack_in_between(monotonic):
    # given
    container = MagicMock()
    async_no_progress_callback = MagicMock()

    # and
    monotonic.side_effect = [17.0, 17.01, 17.02]

    # and
    queue = Queue()
    # First failing batch
    signal_batch_started(queue=queue, occured_at=0.0)
    signal_batch_processed(queue=queue, occured_at=6.0)
    # Proper batch
    signal_batch_started(queue=queue, occured_at=7.0)
    signal_batch_processed(queue=queue, occured_at=9.0)
    # Second failing batch
    signal_batch_started(queue=queue, occured_at=10.0)
    signal_batch_processed(queue=queue, occured_at=16.0)

    # and
    processor = SignalsProcessor(
        period=10,
        container=container,
        queue=queue,
        async_lag_threshold=1.0,
        async_no_progress_threshold=5.0,
        async_no_progress_callback=async_no_progress_callback,
        callbacks_interval=5,
    )

    # when
    processor.work()

    # then
    async_no_progress_callback.assert_called_with(container)
    assert len(monotonic.mock_calls) == 3
    assert len(async_no_progress_callback.mock_calls) == 1


@patch("neptune.internal.signals_processing.signals_processor.monotonic")
def test__no_progress__proper_then_too_long_different_cycles(monotonic):
    # given
    container = MagicMock()
    async_no_progress_callback = MagicMock()

    # and
    monotonic.side_effect = [5.0, 5.01, 16.0, 16.01]

    # and
    queue = Queue()
    # First proper batch
    signal_batch_started(queue=queue, occured_at=0.0)
    signal_batch_processed(queue=queue, occured_at=4.0)
    # Second too long batch
    signal_batch_started(queue=queue, occured_at=5.0)

    # and
    processor = SignalsProcessor(
        period=10,
        container=container,
        queue=queue,
        async_lag_threshold=1.0,
        async_no_progress_threshold=5.0,
        async_no_progress_callback=async_no_progress_callback,
        callbacks_interval=5,
    )

    # when
    processor.work()

    # then
    async_no_progress_callback.assert_not_called()

    # given
    signal_batch_processed(queue=queue, occured_at=15.0)

    # when
    processor.work()

    # then
    async_no_progress_callback.assert_called_once_with(container)

    # and
    assert len(monotonic.mock_calls) == 3
    assert len(async_no_progress_callback.mock_calls) == 1
