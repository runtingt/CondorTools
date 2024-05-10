#! /vols/cms/tr1123/condor_tools/env/bin/python

import htcondor
import subprocess
from typing import DefaultDict
from collections import defaultdict
from termcolor import colored
from prettytable import PrettyTable
import os

# Initialize the Collector and Schedd
collector = htcondor.Collector()
schedd = htcondor.Schedd()

def get_real_name(username: str) -> str:
    """ Uses the 'pinky' command to get the real name of a user from their username """
    try:
        # Parse output of 'pinky' to extract the real name
        result = subprocess.run(['pinky', '-l', username], stdout=subprocess.PIPE, text=True)
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if 'In real life:' in line:
                    return line.split('In real life:')[1].strip()
    except Exception:
        pass
    return ""

# ls -l /vols/lhcb | awk '{print $3}'

def get_experiments(username: str) -> str:
    exps = []
    for exp in os.listdir("/vols"):
        ls = subprocess.run(['ls', '-l', f'/vols/{exp}'], stdout=subprocess.PIPE, text=True)
        lines = ls.stdout.split("\n")

        exp_users = []
        for line in lines:
            fields = list(filter(lambda x: x != "", line.split(" ")))
            if len(fields) >= 9:
                exp_users.append(fields[2])
        if username in exp_users:
            exps.append(exp)
    return exps

def fetch_jobs() -> DefaultDict:
    """ Fetch and print job details from HTCondor schedd, grouped and ranked by user based on job count """
    status_dict = {1: 'Idle', 2: 'Running', 3: 'Removed', 4: 'Completed', 5: 'Held', 6: 'Transferring Output', 7: 'Suspended'}
    
    # Query for jobs
    jobs = schedd.query(
        projection=["ClusterId", "ProcId", "Owner", "JobStatus", "RemoteHost", "RequestGPUs", "RequestCpus"]
    )

    # Group jobs by owner and count statuses, differentiated by machine type
    user_jobs = defaultdict(list)
    user_stats = defaultdict(lambda: {'CPU': defaultdict(int), 'GPU': defaultdict(int), 'Total': defaultdict(int)})
    for job in jobs:
        job_info = {
            "Job ID": f"{job['ClusterId']}.{job['ProcId']}",
            "Status": status_dict.get(job.get("JobStatus"), "Unknown"),
            "Machine": job.get("RemoteHost", "N/A"),
            "SLOTS": job.get("RequestCpus") if isinstance(job.get("RequestCpus", 1), int) else 1
        }
        
        owner = job["Owner"]
        if 'gpu' in job_info["Machine"].lower():
            machine_type = 'GPU'
        elif job_info["Machine"] == "N/A":
            gpus = job.get('RequestGPUs', 0)
            if gpus != 0:
                machine_type = 'GPU'
            else:
                machine_type = 'CPU'
        else:
            machine_type = 'CPU'
        user_jobs[owner].append(job_info)
        user_stats[owner][machine_type][job_info["Status"]] += job_info["SLOTS"]
        user_stats[owner]['Total'][job_info["Status"]] += 1
                
    return user_stats

def format_table(user_stats: DefaultDict) -> PrettyTable:
    # Setup table
    tab = PrettyTable(['User', 'Name' ,'CPU', 'GPU', 'Total'], align='l', hrules=1)
    status_for_print = ['Running', 'Idle', 'Held']
    machine_stats = defaultdict(lambda: dict(zip(status_for_print, [0]*len(status_for_print))))
    
    # Get stats by user, per machine type
    for user, jobs in user_stats.items():
        row = [user]
        row.append(get_real_name(user))
        for machine_type, stats in jobs.items():
            s = ''
            total = 0
            for status in status_for_print:
                val = stats[status]
                s += f"{status}: {val}\n"
                total += val
                machine_stats[machine_type][status] += val
            s += f"Total: {total}"
            row.append(s)
        tab.add_row(row)
    
    # Get totals by machine type
    totals = []
    for stats in machine_stats.values():
        s = ''
        for status in status_for_print:
            s += colored(f"{status}: {stats[status]}", 'red') + '\n'
        s += colored(f"Total: {sum(stats.values())}", 'red')
        totals.append(s)
    tab.add_row([colored('Total', 'red'), ''] + totals)
    
    return tab

def better_table(user_stats: DefaultDict) -> PrettyTable:
    # Setup table
    tab = PrettyTable(['User', 'Name' ,'Running', 'Idle', 'Held', 'Total'], align='l', hrules=1)
    status_for_print = ['Running', 'Idle', 'Held']
    
    # Get stats by user, per machine type
    for user, jobs in user_stats.items():
        experiments = get_experiments(user)
        name = get_real_name(user) + " (%s)"%(", ".join(experiments))
        row = [user, name, 
               jobs["CPU"]["Running"],
               jobs["CPU"]["Idle"],
               jobs["CPU"]["Held"]]
        row.append(sum(row[-3:]))
        tab.add_row(row)
    
    # # Get totals by machine type
    # totals = []
    # for stats in machine_stats.values():
    #     s = ''
    #     for status in status_for_print:
    #         s += colored(f"{status}: {stats[status]}", 'red') + '\n'
    #     s += colored(f"Total: {sum(stats.values())}", 'red')
    #     totals.append(s)
    # tab.add_row([colored('Total', 'red'), ''] + totals)
    
    return tab

if __name__ == "__main__":
    user_stats = fetch_jobs()
    table = better_table(user_stats)
    print(table)
