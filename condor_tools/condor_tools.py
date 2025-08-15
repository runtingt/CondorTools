import argparse
import datetime
import getpass
import logging
import os
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

import htcondor
from prettytable import PrettyTable
from termcolor import colored

STATUSES_TO_PRINT = ["Running", "Idle", "Held"]


# Setup logging
class CustomFormatter(logging.Formatter):
    """Custom formatter to allow plain text output"""

    def format(self, record):
        if hasattr(record, "simple") and record.simple:
            return record.getMessage()
        return super().format(record)


handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter(fmt="%(levelname)s: %(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logging.basicConfig(level=logging.INFO, handlers=[handler])


def _setup_condor() -> tuple:
    # Initialize the Collector and Schedd
    collector = htcondor.Collector()
    schedd = htcondor.Schedd()
    return collector, schedd


def _get_real_name(username: str) -> str:
    """Uses the 'pinky' command to get the real name of a user from their username"""
    try:
        # Parse output of 'pinky' to extract the real name
        result = subprocess.run(["pinky", "-l", username], check=False, stdout=subprocess.PIPE, text=True)
        if result.returncode == 0:
            for line in result.stdout.split("\n"):
                if "In real life:" in line:
                    return line.split("In real life:")[1].strip()
    except Exception:
        pass
    return ""


def fetch_jobs(only: str, schedd) -> defaultdict:
    """Fetch and print job details from HTCondor schedd, grouped and ranked by user based on job count"""
    status_dict = {
        1: "Idle",
        2: "Running",
        3: "Removed",
        4: "Completed",
        5: "Held",
        6: "Transferring Output",
        7: "Suspended",
    }

    # Query for jobs
    jobs = schedd.query(projection=["ClusterId", "ProcId", "Owner", "JobStatus", "RemoteHost", "RequestGPUs"])

    # Group jobs by owner and count statuses, differentiated by machine type
    user_jobs = defaultdict(list)
    user_stats = defaultdict(lambda: {"CPU": defaultdict(int), "GPU": defaultdict(int), "Total": defaultdict(int)})
    for job in jobs:
        job_info = {
            "Job ID": f"{job['ClusterId']}.{job['ProcId']}",
            "Status": status_dict.get(job.get("JobStatus"), "Unknown"),
            "Machine": job.get("RemoteHost", "N/A"),
        }
        owner = job["Owner"]
        if "gpu" in job_info["Machine"].lower():
            machine_type = "GPU"
        elif job_info["Machine"] == "N/A":
            gpus = job.get("RequestGPUs", 0)
            if gpus != 0:
                machine_type = "GPU"
            else:
                machine_type = "CPU"
        else:
            machine_type = "CPU"

        # Filter on machine type if specified
        if only and machine_type.lower() != only.lower():
            continue

        user_jobs[owner].append(job_info)
        user_stats[owner][machine_type][job_info["Status"]] += 1
        user_stats[owner]["Total"][job_info["Status"]] += 1

    return user_stats


def _get_headers(priority: bool, only: str):
    if priority:
        headers = ["User", "Name", "Priority", "CPU", "GPU", "Total"]
    else:
        headers = ["User", "Name", "CPU", "GPU", "Total"]
    if only:
        headers.remove("Total")
        if only.lower() == "cpu":
            headers.remove("GPU")
        elif only.lower() == "gpu":
            headers.remove("CPU")
    return headers


@dataclass
class TableContext:
    """Context object to hold table formatting parameters."""

    current_user: str
    current_date: str
    only: str
    user_priorities: dict[str, float]
    priority: bool


def _build_machine_stats_string(machine_type: str, stats: defaultdict, machine_stats: defaultdict) -> str:
    """Build the summary statistics string for a machine type."""
    s = ""
    total = 0
    for status in STATUSES_TO_PRINT:
        val = stats[status]
        s += f"{status}: {val}\n"
        total += val
        machine_stats[machine_type][status] += val
    s += f"Total: {total}"
    return s


def _get_row(user: str, jobs: defaultdict, ctx: TableContext) -> tuple[list[str], defaultdict]:
    """Generate a table row for a user with their job statistics."""
    row = [user, _get_real_name(user)]
    machine_stats = defaultdict(lambda: dict(zip(STATUSES_TO_PRINT, [0] * len(STATUSES_TO_PRINT))))

    # ;)
    if user == "gu18" and user == ctx.current_user and ctx.current_date == "01/04":
        for stats in jobs.values():
            total = sum(stats.values())
            stats["Idle"] = 0
            stats["Running"] = 0
            stats["Held"] = total

    if ctx.priority:
        row.append(ctx.user_priorities.get(user, -1))

    for machine_type, stats in jobs.items():
        if ctx.only and machine_type.lower() != ctx.only.lower():
            continue

        stats_string = _build_machine_stats_string(machine_type, stats, machine_stats)
        row.append(stats_string)

    return row, machine_stats


def _highlight_row(user: str, current_user: str, row: list[str]) -> list[str]:
    if user == current_user:
        c_row = []
        for entry in row:
            # Apply color to the text content only, not the dividers
            colored_lines = [colored(line, "green") for line in str(entry).split("\n")]
            colored_entry = "\n".join(colored_lines)
            c_row.append(colored_entry)
        return c_row
    return row


def format_table(
    user_stats: defaultdict,
    only: str,
    user_priorities: dict[str, float],
    current_user: Optional[str] = None,
    priority: bool = False,
) -> PrettyTable:
    """Format job statistics into a table."""
    headers = _get_headers(priority, only)
    current_date = datetime.datetime.now().strftime("%d/%m")
    tab = PrettyTable(headers, align="l", hrules=1)

    # Create context object to reduce parameter passing
    ctx = TableContext(
        current_user=current_user or "",
        current_date=current_date,
        only=only,
        user_priorities=user_priorities,
        priority=priority,
    )

    machine_stats = defaultdict(lambda: dict(zip(STATUSES_TO_PRINT, [0] * len(STATUSES_TO_PRINT))))

    # Get stats by user, per machine type
    for user, jobs in user_stats.items():
        row, user_machine_stats = _get_row(user, jobs, ctx)

        # Accumulate machine stats for totals
        for machine_type, stats in user_machine_stats.items():
            for status in STATUSES_TO_PRINT:
                machine_stats[machine_type][status] += stats[status]

        tab.add_row(_highlight_row(user, current_user, row))

    # Get totals by machine type
    totals = []
    for stats in machine_stats.values():
        s = ""
        for status in STATUSES_TO_PRINT:
            s += colored(f"{status}: {stats[status]}", "red") + "\n"
        s += colored(f"Total: {sum(stats.values())}", "red")
        totals.append(s)

    # Add totals row
    try:
        if priority:
            tab.add_row([colored("Total", "red"), "", "", *totals])
        else:
            tab.add_row([colored("Total", "red"), "", *totals])
    except ValueError:
        logging.warning("No jobs found in current schedd.")
        sys.exit(1)

    return tab


def log():
    """Logs the usage of the script"""
    # Get the current user's username
    username = getpass.getuser()
    real_name = _get_real_name(username)

    # Get the current timestamp
    timestamp = datetime.datetime.now().isoformat()

    # Log to file
    script_dir = os.path.dirname(os.path.realpath(__file__))
    log_file_path = os.path.join(script_dir, "usage_log.txt")
    with open(log_file_path, "a+") as log_file:
        # Write the timestamp, username, and real name to the log file
        log_file.write(f"{timestamp}, {username}, {real_name}\n")


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Display HTCondor job stats.")
    parser.add_argument("--priority", action="store_true", help="Display user priorities.")
    parser.add_argument("--only", choices=["cpu", "gpu"], help="Filter jobs by machine type (CPU or GPU).")
    args = parser.parse_args()
    priority = args.priority
    logging.info("HTCondor Job Stats")

    # Get the user who ran the script
    username = getpass.getuser()

    if priority:
        # Get user priorities from condor_userprio --allusers
        # because the AdTypes.Negotiator leaves out some users
        user_priorities = defaultdict(float)
        result = subprocess.run(["condor_userprio", "-allusers", "-priority"], check=False, stdout=subprocess.PIPE)
        lines = result.stdout.decode("utf-8").split("\n")[4:-3]  # Cursed
        MIN_USERPRIO_PARTS = 3
        for line in lines:
            parts = line.split()
            if len(parts) >= MIN_USERPRIO_PARTS:
                user_priorities[parts[0].split("@")[0]] = float(parts[1])  # Use the second column as the priority
    else:
        user_priorities = {}

    log()
    user_stats = fetch_jobs(args.only, _setup_condor()[1])
    table = format_table(user_stats, args.only, user_priorities, current_user=username, priority=priority)
    logging.info(table, extra={"simple": True})
