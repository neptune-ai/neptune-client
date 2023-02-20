import pytest

from neptune.exceptions import NeptuneException


@pytest.mark.parametrize("state", ["active", "inactive", "Active", "Inactive", "aCTive", "INacTiVe"])
def test_fetch_runs_table_is_case_insensitive(state, project):
    try:
        project.fetch_runs_table(state=state)
        assert True
    except Exception as e:
        assert False, e


def test_fetch_runs_table_raises_correct_exception_if_state_incorrect(project):
    with pytest.raises(NeptuneException):
        project.fetch_runs_table(state="some_incorrect_state")
