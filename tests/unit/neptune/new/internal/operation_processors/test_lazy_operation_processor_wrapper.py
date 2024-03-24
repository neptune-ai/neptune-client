import mock

from neptune.internal.operation_processors.lazy_operation_processor_wrapper import LazyOperationProcessorWrapper
from neptune.internal.operation_processors.operation_processor import OperationProcessor


@mock.patch("neptune.internal.operation_processors.lazy_operation_processor_wrapper.get_operation_processor")
def test_lazy_initialization(get_operation_processor):
    # when
    lazy_wrapper = LazyOperationProcessorWrapper()

    # then
    get_operation_processor.assert_not_called()
    assert not lazy_wrapper.evaluated

    # when
    lazy_wrapper.enqueue_operation(mock.Mock(), wait=False)
    lazy_wrapper.enqueue_operation(mock.Mock(), wait=False)

    # then
    get_operation_processor.assert_called_once()
    assert lazy_wrapper.evaluated


@mock.patch("neptune.internal.operation_processors.lazy_operation_processor_wrapper.get_operation_processor")
def test_call_propagation_to_wrapped(get_operation_processor):
    # given
    operation_processor = mock.Mock(spec=OperationProcessor)
    get_operation_processor.return_value = operation_processor
    lazy_wrapper = LazyOperationProcessorWrapper(some_kwarg="some")

    # when
    arg_mock = mock.Mock()
    lazy_wrapper.enqueue_operation(arg_mock, wait=True)

    # then
    get_operation_processor.assert_called_once_with(some_kwarg="some")
    operation_processor.enqueue_operation.assert_called_once_with(arg_mock, wait=True)

    # when
    with mock.patch.object(
        LazyOperationProcessorWrapper, "operation_storage", new_callable=mock.PropertyMock
    ) as operation_storage:
        _ = lazy_wrapper.operation_storage

    # then
    operation_storage.assert_called_once()

    for method in ["pause", "resume", "flush", "wait", "stop", "close"]:
        # when
        getattr(lazy_wrapper, method)()

        # then
        getattr(operation_processor, method).assert_called_once()


@mock.patch("neptune.internal.operation_processors.lazy_operation_processor_wrapper.get_operation_processor")
def test_post_init_trigger_start_called(get_operation_processor):
    # given
    operation_processor = mock.Mock(spec=OperationProcessor)
    get_operation_processor.return_value = operation_processor
    lazy_wrapper = LazyOperationProcessorWrapper()

    # when
    lazy_wrapper.enqueue_operation(mock.Mock(), wait=False)

    # then
    operation_processor.start.assert_called_once()
