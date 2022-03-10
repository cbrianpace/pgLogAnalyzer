# pgLogAnalyzer


## Introduction
This solution is used to load pgBouncer, Crunchy PostgreSQL Operator, and PostgreSQL logs into Granafa's Loki solution for quick analysis.  Once the stack is started, the loadLogs.py python program can be used to load log files in for analysis.

## Requirements
- Docker Desktop
- Python 3.9.10+
- Following Python Modules
  - pytz
  - requests
  - glob
  - argparse
  - re

## Setup
The following steps can be followed to start the stack in Docker


## Loading Logs
The load the logs into Loki, execute the following:

```
python3 loadLogs.py -d <directory contain log files> [-t <postgres|pgbouncer|pgo>]
```

The directory passed to the program will be recursively searched for all *.log files.  If the type was not specified (using -t), then the program will attempt to determine the log type from the log name.

## Example Queries
```
{logtype="pgbouncer"} |~ "LOG stats" | regexp "(xacts/s, (?P<queries>\\S* ))" 

{logtype="pgbouncer"} |~ "LOG stats" | pattern "<_> <_> stats: <xacts> xacts/s, <queries> queries/s, in <inbytesps> B/s, out <outbytesps> B/s, xact <xactus> us, query <queryus> us, wait <waitus> us"

sum by (target) (sum_over_time({logtype="pgbouncer"} |~ "LOG stats" | pattern "<_> <_> stats: <xacts> xacts/s, <queries> queries/s, in <inbytesps> B/s, out <outbytesps> B/s, xact <xactus> us, query <queryus> us, wait <waitus> us" | unwrap inbytesps [1m]))

{target=~"video.+", logtype="postgres"} | pattern "[<_>] <level>:"
```