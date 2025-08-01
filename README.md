# Condor Tools

Useful script(s) for viewing statistics about a HTCondor pool.

## Notes

- Running `condor_stat.py` will include `condor_dagman` jobs in the output, which are hidden by default in `condor_q`. If you see a discrepancy between the number of jobs in `condor_q` and `condor_stat.py`, this is likely the reason. To check, run `condor_q -nobatch` to show all jobs, including `condor_dagman` jobs.
