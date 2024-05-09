import htcondor
import subprocess
from collections import defaultdict

# Initialize the Collector and Schedd
collector = htcondor.Collector()
schedd = htcondor.Schedd()

def get_real_name(username):
    """ Uses the 'pinky' command to get the real name of a user from their username """
    try:
        # Running pinky command to get full name
        result = subprocess.run(['pinky', '-l', username], stdout=subprocess.PIPE, text=True)
        # Parse output to extract the real name
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if 'In real life:' in line:
                    return line.split('In real life:')[1].strip()
    except Exception as e:
        pass
    return ""

def fetch_jobs():
    """ Fetch and print job details from HTCondor schedd, grouped and ranked by user based on job count """
    # Query for jobs
    jobs = schedd.query(
        projection=["ClusterId", "ProcId", "Owner", "JobStatus", "RemoteHost"]
    )

    # Job status dictionary for better readability
    status_dict = {1: 'Idle', 2: 'Running', 3: 'Removed', 4: 'Completed', 5: 'Held', 6: 'Transferring Output', 7: 'Suspended'}

    # Group jobs by owner and count statuses, differentiated by machine type
    user_jobs = defaultdict(list)
    user_stats = defaultdict(lambda: {'CPU': defaultdict(int), 'GPU': defaultdict(int), 'Unknown': defaultdict(int), 'Total': defaultdict(int)})
    for job in jobs:
        job_info = {
            "Job ID": f"{job['ClusterId']}.{job['ProcId']}",
            "Status": status_dict.get(job.get("JobStatus"), "Unknown"),
            "Machine": job.get("RemoteHost", "N/A")
        }
        owner = job["Owner"]
        if 'gpu' in job_info["Machine"].lower():
            machine_type = 'GPU'
        elif job_info["Machine"] == "N/A":
            machine_type = 'Unknown'
        else:
            machine_type = 'CPU'
        user_jobs[owner].append(job_info)
        user_stats[owner][machine_type][job_info["Status"]] += 1
        user_stats[owner]['Total'][job_info["Status"]] += 1

    # Calculate total job counts per user and rank
    total_job_counts = {user: sum(stats['Total'].values()) for user, stats in user_stats.items()}
    ranked_users = sorted(total_job_counts, key=total_job_counts.get, reverse=True)

    # Print jobs and summary statistics grouped by owner, ranked by total job count
    for owner in ranked_users:
        real_name = get_real_name(owner)
        print(f"{owner} ({real_name}) - (Total Jobs: {total_job_counts[owner]}):")
        jobs = user_jobs[owner]
#        for job in jobs:
#            print(f"  Job ID: {job['Job ID']}, Status: {job['Status']}, Machine: {job['Machine']}")
        # Print detailed and total summary statistics for each user
#        print("  Detailed Summary Statistics:")
        for machine_type, stats in user_stats[owner].items():
            if len(stats.values()) != 0:
                s = f"  {machine_type}: "
                for status, count in stats.items():
                    s += f"{status}: {count}, "
                s = s[:-2]
                print(s)

def fetch_machines():
    """ Fetch and print machine details from HTCondor collector grouped by type (CPU or GPU) """
    # Query for machine statuses
    machines = collector.query(
        htcondor.AdTypes.Startd,
        projection=["Name", "State", "Activity", "Machine"]
    )

    # Group machines by type
    machine_stats = {'CPU': [], 'GPU': []}
    for machine in machines:
        machine_type = 'GPU' if 'gpu' in machine['Name'].lower() else 'CPU'
        machine_stats[machine_type].append(machine)

    # Print machine details
    for machine_type, machines in machine_stats.items():
        print(f"\n{machine_type} Machines:")
        for machine in machines:
            print(f"Machine Name: {machine['Name']}, State: {machine['State']}, Activity: {machine['Activity']}")

def main():
#    print("Fetching HTCondor Job Information...")
    fetch_jobs()
#    print("\nFetching HTCondor Machine Information...")
#    fetch_machines()

if __name__ == "__main__":
    main()
