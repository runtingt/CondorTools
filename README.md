# Condor Tools

[![python version](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/) [![codecov](https://codecov.io/gh/runtingt/CondorTools/graph/badge.svg?token=JBIJUS9P00)](https://codecov.io/gh/runtingt/CondorTools) [![CI](https://github.com/runtingt/CondorTools/actions/workflows/ci.yaml/badge.svg)](https://github.com/runtingt/CondorTools/actions/workflows/ci.yaml)

Useful script(s) for viewing statistics about a HTCondor pool.

## Usage

There are various options available for the `condor_stat.py` script - see the help message for details:

```bash
$ ./condor_stat.py --help
usage: condor_stat.py [-h] [--priority] [--only {cpu,gpu}]

Display HTCondor job stats.

optional arguments:
  -h, --help        show this help message and exit
  --priority        Display user priorities.
  --only {cpu,gpu}  Filter jobs by machine type (CPU or GPU).
```

## Notes

- Running `condor_stat.py` will include `condor_dagman` jobs in the output, which are hidden by default in `condor_q`. If you see a discrepancy between the number of jobs in `condor_q` and `condor_stat.py`, this is likely the reason. To check, run `condor_q -nobatch` to show all jobs, including `condor_dagman` jobs.
