import unittest
from unittest.mock import (
    MagicMock,
    patch,
)

from neptune.internal.utils.dependency_tracking import (
    FileDependenciesStrategy,
    InferDependenciesStrategy,
)


class TestDependencyTracking(unittest.TestCase):
    @patch("subprocess.check_output", return_value=b"some_dependency==1.0.0\n")
    @patch("neptune.types.File.from_content")
    def test_infer_calls_upload_correctly(self, mock_from_content, mock_check_output):
        InferDependenciesStrategy(run=MagicMock()).track_dependencies()

        mock_check_output.assert_called_once()
        mock_from_content.assert_called_once_with("some_dependency==1.0.0\n")

    @patch("subprocess.check_output", return_value=b"")
    @patch("neptune.types.File.from_content")
    def test_infer_does_not_upload_empty_dependency_string(self, mock_from_content, mock_check_output):
        InferDependenciesStrategy(run=MagicMock()).track_dependencies()

        mock_check_output.assert_called_once()
        mock_from_content.assert_not_called()

    @patch("neptune.handler.Handler.upload_files")
    def test_file_strategy_not_uploading_if_path_incorrect(self, mock_upload_files):
        FileDependenciesStrategy(run=MagicMock(), path="non-existent_file_path.txt").track_dependencies()

        mock_upload_files.assert_not_called()

    @patch("os.path.isfile", return_value=True)
    def test_file_strategy_uploads_correct_path(self, mock_is_file):
        run = MagicMock()
        handler = MagicMock()
        run.__getitem__ = MagicMock()
        run.__getitem__.return_value = handler
        handler.upload_files = MagicMock()

        FileDependenciesStrategy(run=run, path="valid_file_path.txt").track_dependencies()

        handler.upload_files.assert_called_once_with("valid_file_path.txt")
