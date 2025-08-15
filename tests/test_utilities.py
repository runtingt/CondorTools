from collections import defaultdict
from unittest.mock import MagicMock, patch

import pytest

from ..condor_tools.condor_tools import (
    TableContext,
    _build_machine_stats_string,
    _get_headers,
    _get_real_name,
    _get_row,
    _highlight_row,
    _setup_condor,
)

test_context = TableContext(
    current_user="test_user0",
    current_date="2023-10-01",
    only=None,
    user_priorities={"test_user0": 1.0, "test_user1": 1.1, "gu18": 0.5},
    priority=False,
)


class TestCondorSetup:
    with patch("htcondor.Schedd", return_value=MagicMock()) as mock_schedd:
        _setup_condor()
        mock_schedd.assert_called_once()


class TestGetRealName:
    """Test cases for _get_real_name function."""

    @pytest.mark.parametrize("username, expected", [("testuser", "???"), (None, "")])
    def test_get_real_name(self, username, expected):
        result = _get_real_name(username)
        assert result == expected


class TestHeaders:
    @pytest.mark.parametrize("priority", [True, False])
    @pytest.mark.parametrize("only", ["cpu", "gpu", None])
    def test_headers(self, priority, only):
        headers = _get_headers(priority, only)
        if priority:
            assert "Priority" in headers
        if only:
            assert "Total" not in headers
            if only.lower() == "cpu":
                assert "GPU" not in headers
            elif only.lower() == "gpu":
                assert "CPU" not in headers
        else:
            assert "Total" in headers
            assert "CPU" in headers
            assert "GPU" in headers


class TestMachineStats:
    def test_build_machine_stats_string(self):
        stats = {
            "Running": 1,
            "Idle": 2,
            "Held": 3,
        }
        machine_type = "test_machine"

        machine_stats = defaultdict(lambda: defaultdict(int))
        result = _build_machine_stats_string(machine_type, stats, machine_stats)
        assert "Total: 6" in result
        assert machine_stats[machine_type] == stats


class TestGetRow:
    @pytest.mark.parametrize("priority", [True, False])
    @pytest.mark.parametrize("only", ["cpu", "gpu", None])
    @pytest.mark.parametrize("april_fools", [True, False])
    @pytest.mark.parametrize("user", ["test_user0", "gu18"])
    def test_get_row(self, priority, only, april_fools, user):
        test_context.priority = priority
        test_context.only = only
        if april_fools:
            test_context.current_user = "gu18"
            test_context.current_date = "01/04"

        jobs = {"cpu": {"Running": 1, "Idle": 2, "Held": 3}, "gpu": {"Running": 0, "Idle": 1, "Held": 0}}

        row, machine_stats = _get_row(user=user, jobs=jobs, ctx=test_context)
        assert row[0] == user
        if user == "test_user0":
            assert row[1] == "???"
        if priority:
            assert row[2] == test_context.user_priorities[user]
            if only:
                if only.lower() == "cpu":
                    assert row[3] == _build_machine_stats_string("cpu", jobs["cpu"], machine_stats)
                elif only.lower() == "gpu":
                    assert row[3] == _build_machine_stats_string("gpu", jobs["gpu"], machine_stats)
            else:
                assert row[3] == _build_machine_stats_string("cpu", jobs["cpu"], machine_stats)
                assert row[4] == _build_machine_stats_string("gpu", jobs["gpu"], machine_stats)
        elif only:
            if only.lower() == "cpu":
                assert row[2] == _build_machine_stats_string("cpu", jobs["cpu"], machine_stats)
            elif only.lower() == "gpu":
                assert row[2] == _build_machine_stats_string("gpu", jobs["gpu"], machine_stats)
        else:
            assert row[2] == _build_machine_stats_string("cpu", jobs["cpu"], machine_stats)
            assert row[3] == _build_machine_stats_string("gpu", jobs["gpu"], machine_stats)


class TestHighlightRow:
    @pytest.mark.parametrize("current_user", ["test_user0", "test_user1"])
    def test_highlight_row(self, current_user):
        user = "test_user0"
        test_context.current_user = current_user
        row = _get_row(
            user=user,
            jobs={"cpu": {"Running": 1, "Idle": 2, "Held": 3}, "gpu": {"Running": 0, "Idle": 1, "Held": 0}},
            ctx=test_context,
        )[0]
        h_row = _highlight_row(user, test_context.current_user, row)

        if user != current_user:
            assert h_row == row
        else:
            assert h_row[0] == f"\033[32m{user}\033[0m"
