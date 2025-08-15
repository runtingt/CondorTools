import datetime
import getpass
import os
import subprocess
import sys

import pytest

from ..condor_tools import condor_tools


def test_log_writes_to_file(fs, monkeypatch):
    """
    Test the log() function without touching the real filesystem.
    """

    # Arrange: Patch getpass.getuser and _get_real_name to known values
    monkeypatch.setattr(getpass, "getuser", lambda: "testuser")
    monkeypatch.setattr(condor_tools, "_get_real_name", lambda u: "Test User")

    # Arrange: Patch datetime so the timestamp is fixed
    fixed_time = datetime.datetime(2025, 1, 1, 12, 0, 0)
    monkeypatch.setattr(datetime, "datetime", type("dt", (), {
        "now": staticmethod(lambda: fixed_time),
        "isoformat": datetime.datetime.isoformat
    }))

    # Arrange: Ensure __file__ location exists in fake FS
    script_dir = "/fake/dir"
    fs.create_dir(script_dir)
    monkeypatch.setattr(condor_tools, "__file__", os.path.join(script_dir, "myscript.py"))

    # Act
    condor_tools.log()

    # Assert: Check file contents
    log_path = os.path.join(script_dir, "usage_log.txt")
    with open(log_path) as f:
        content = f.read()

    assert content == "2025-01-01T12:00:00, testuser, Test User\n"
    
    
@pytest.mark.parametrize("priority", [True, False])
def test_main_with_priority(monkeypatch, caplog, priority):
    # Fake a bunch of stuff
    fake_output = b"""
    some header
    more header
    even more header
    skip me
    alice@domain.com  10.5  something
    bob@domain.com    5.0   something
    trailing line
    footer line
    footer line 2
    """
    if priority:
        monkeypatch.setattr(sys, "argv", ["script.py", "--priority", "--only", "cpu"])
    else:
        monkeypatch.setattr(sys, "argv", ["script.py", "--only", "cpu"])
    monkeypatch.setattr(getpass, "getuser", lambda: "testuser")
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **kw: subprocess.CompletedProcess(args=a, returncode=0, stdout=fake_output)
    )
    monkeypatch.setattr(condor_tools, "log", lambda: None)
    monkeypatch.setattr(condor_tools, "_setup_condor", lambda: (None, "schedd"))
    monkeypatch.setattr(condor_tools, "fetch_jobs", lambda only, schedd: {"job": {"some": "stats"}})
    monkeypatch.setattr(condor_tools, "format_table", lambda *a, **k: "formatted table")
    caplog.set_level("INFO")

    condor_tools.main()
    assert "HTCondor Job Stats" in caplog.text
    assert "formatted table" in caplog.text
