import pytest

from ..condor_tools.condor_tools import fetch_jobs

TEST_JOBS = [
    {
        "Owner": "test_user0",
        "ProcId": 0,
        "ClusterId": 12345,
        "JobStatus": 1,
        "RemoteHost": "cpu1.example.com",
        "RequestGPUs": 0
    },
    {
        "Owner": "test_user0",
        "ProcId": 1,
        "ClusterId": 12345,
        "JobStatus": 2,
        "RemoteHost": "cpu2.example.com",
        "RequestGPUs": 0
    },
    {
        "Owner": "test_user0",
        "ProcId": 1,
        "ClusterId": 12345,
        "JobStatus": 5,
        "RequestGPUs": 0
    },
    {
        "Owner": "test_user0",
        "ProcId": 1,
        "ClusterId": 12345,
        "JobStatus": 5,
        "RequestGPUs": 1
    },
    {
        "Owner": "test_user1",
        "ProcId": 0,
        "ClusterId": 12346,
        "JobStatus": 1,
        "RemoteHost": "gpu1.example.com",
        "RequestGPUs": 1
    },
    {
        "Owner": "test_user1",
        "ProcId": 1,
        "ClusterId": 12346,
        "JobStatus": 2,
        "RemoteHost": "gpu2.example.com",
        "RequestGPUs": 1
    },
]

EXPECTED_RESULT = {
    "test_user0": {
        "CPU": {"Idle": 1, "Running": 1, "Held": 1},
        "GPU": {"Held": 1},
        "Total": {"Idle": 1, "Running": 1, "Held": 2}
    },
    "test_user1": {
        "CPU": {},
        "GPU": {"Idle": 1, "Running": 1},
        "Total": {"Idle": 1, "Running": 1}
    }
}

class TestFetchJobs:
    @pytest.mark.parametrize("only", [None, "cpu"])
    def test_fetch_jobs(self, mocker, only):
        mock_schedd = mocker.Mock()
        mock_schedd.query.return_value = TEST_JOBS
        
        result = fetch_jobs(only, mock_schedd)
        result = {user: {k: dict(v) for k, v in stats.items()} for user, stats in result.items()}
        if only:
            expected_result = {
                user: {k: v for k, v in stats.items() if k != "GPU"}
                for user, stats in EXPECTED_RESULT.items() 
                if stats[only.upper()] != {}
            }
            for results in expected_result.values():
                results["Total"] = results[only.upper()]
                results["GPU"] = {}
        else:
            expected_result = EXPECTED_RESULT
        assert result == expected_result