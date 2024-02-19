import unittest.mock as mock

from neptune.internal.operation_processors.lazy_operation_processor_wrapper import LazyOperationProcessorWrapper
from neptune.internal.operation_processors.operation_processor import OperationProcessor


def test_lazy_initialization():
    # given
    operation_processor = mock.Mock(spec=OperationProcessor)
    operation_processor_getter = mock.Mock(return_value=operation_processor)

    # when
    lazy_wrapper = LazyOperationProcessorWrapper(operation_processor_getter)

    # then
    operation_processor_getter.assert_not_called()
    assert not lazy_wrapper.evaluated

    # when
    lazy_wrapper.enqueue_operation(mock.Mock(), wait=False)
    lazy_wrapper.enqueue_operation(mock.Mock(), wait=False)

    # then
    operation_processor_getter.assert_called_once()
    assert lazy_wrapper.evaluated


def test_call_propagation_to_wrapped():
    # given
    operation_processor = mock.Mock(spec=OperationProcessor)
    operation_processor_getter = mock.Mock(return_value=operation_processor)
    lazy_wrapper = LazyOperationProcessorWrapper(operation_processor_getter)

    # when
    arg_mock = mock.Mock()
    lazy_wrapper.enqueue_operation(arg_mock, wait=True)

    # then
    operation_processor.enqueue_operation.assert_called_once_with(arg_mock, wait=True)

    # when
    with mock.patch.object(
        LazyOperationProcessorWrapper, "operation_storage", new_callable=mock.PropertyMock
    ) as operation_storage:
        lazy_wrapper.operation_storage

    # then
    operation_storage.assert_called_once()

    for method in ["start", "pause", "resume", "flush", "wait", "stop", "close"]:
        # when
        getattr(lazy_wrapper, method)()

        # then
        getattr(operation_processor, method).assert_called_once()


def test_post_init_trigger_side_effect_called():

    # given
    operation_processor = mock.Mock(spec=OperationProcessor)
    operation_processor_getter = mock.Mock(return_value=operation_processor)
    post_trigger_side_effect = mock.Mock()
    lazy_wrapper = LazyOperationProcessorWrapper(operation_processor_getter, post_trigger_side_effect)

    # when
    lazy_wrapper.enqueue_operation(mock.Mock(), wait=False)

    # then
    post_trigger_side_effect.assert_called_once()
