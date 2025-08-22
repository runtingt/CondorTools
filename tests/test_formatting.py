import pytest

from ..condor_tools.condor_tools import fetch_jobs, format_table
from .test_htcondor import TEST_JOBS
from .test_utilities import fake_groups  # noqa: F401


class TestFormatTable:
    @pytest.mark.parametrize("only", [None, "cpu"])
    @pytest.mark.parametrize("priority", [True, False])
    def test_format_table(self, mocker, only, priority):
        mock_schedd = mocker.Mock()
        mock_schedd.query.return_value = TEST_JOBS
        user_stats = fetch_jobs(None, mock_schedd)

        current_user = "test_user0"
        tab = format_table(
            user_stats=user_stats,
            current_user=current_user,
            only=only,
            priority=priority,
            user_priorities={"test_user0": 1.0, "test_user1": 2.0},
        )

        assert tab is not None
        assert len(tab._rows) == len(user_stats) + 1
        if priority:
            assert "Priority" in tab.field_names
        if only:
            if only.lower() == "cpu":
                assert "GPU" not in tab.field_names
            elif only.lower() == "gpu":
                assert "CPU" not in tab.field_names

    def test_format_table_empty(self):
        with pytest.raises(SystemExit) as excinfo:
            format_table(user_stats={}, current_user="test_user0", only=None, priority=False, user_priorities={})
            assert excinfo.value.code == 1
