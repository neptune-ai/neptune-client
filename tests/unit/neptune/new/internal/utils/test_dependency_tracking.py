import itertools
from unittest.mock import (
    MagicMock,
    patch,
)

import pytest

from neptune.internal.utils.dependency_tracking import (
    FileDependenciesStrategy,
    InferDependenciesStrategy,
)


class TestDependencyTracking:
    @patch("neptune.internal.utils.dependency_tracking.distributions")
    @patch("neptune.types.File.from_content")
    def test_infer_calls_upload_correctly(self, mock_from_content, mock_distributions):
        single_dist = MagicMock()
        single_dist.metadata = {"Name": "some_dependency", "Version": "1.0.0"}
        mock_distributions.return_value = itertools.chain([single_dist])
        InferDependenciesStrategy().log_dependencies(run=MagicMock())

        mock_distributions.assert_called_once()
        mock_from_content.assert_called_once_with("some_dependency==1.0.0")

    @patch("neptune.internal.utils.dependency_tracking.distributions", return_value=[])
    @patch("neptune.types.File.from_content")
    def test_infer_does_not_upload_empty_dependency_string(self, mock_from_content, mock_distributions):
        InferDependenciesStrategy().log_dependencies(run=MagicMock())

        mock_distributions.assert_called_once()
        mock_from_content.assert_not_called()

    @patch("neptune.handler.Handler.upload")
    @patch("neptune.internal.utils.dependency_tracking.logger")
    def test_file_strategy_path_incorrect(self, mock_logger, mock_upload):
        FileDependenciesStrategy(path="non-existent_file_path.txt").log_dependencies(run=MagicMock())

        mock_upload.assert_not_called()
        mock_logger.error.assert_called_once()

    @pytest.mark.parametrize("path", ["valid_file_path.txt", "dir/valid_file_path.txt"])
    @patch("os.path.isfile", return_value=True)
    def test_file_strategy_uploads_correct_path(self, mock_is_file, path):
        run = MagicMock()
        handler = MagicMock()
        run.__getitem__ = MagicMock()
        run.__getitem__.return_value = handler
        handler.upload = MagicMock()

        FileDependenciesStrategy(path=path).log_dependencies(run=run)

        handler.upload.assert_called_once_with(path)
