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
- PostgreSQL Logs (log_line_prefix) must start with %m formated date

## Setup
The first step is to start the pgLogAnalyzer stack:

```
docker-compose -f docker-compose.yaml up
```


## Loading Logs
The load the logs into Loki, execute the following:

```
python3 loadLogs.py -d <directory contain log files> [-t <postgres|pgbouncer|pgo|syslog>][--timezone="+00:00"][-f <m|t>]
```

The directory passed to the program will be recursively searched for all *.log files.  If the type was not specified (using -t), then the program will attempt to determine the log type from the log name.  The first child directory under the specified directory is used as the target label to group related log files.

For timezone (-z or --timezone) pass in the offset from UTC.  This is only used for parsing syslog messages.

Parsing PostgreSQL logs assumes a date format of %m.  If %t is used instead, then override the default assumption using the -f option and specifying 't' as the format.

## Example Queries
```
{logtype="pgbouncer"} |~ "LOG stats" | regexp "(xacts/s, (?P<queries>\\S* ))" 

{logtype="pgbouncer"} |~ "LOG stats" | pattern "<_> <_> stats: <xacts> xacts/s, <queries> queries/s, in <inbytesps> B/s, out <outbytesps> B/s, xact <xactus> us, query <queryus> us, wait <waitus> us"

sum by (target) (sum_over_time({logtype="pgbouncer"} |~ "LOG stats" | pattern "<_> <_> stats: <xacts> xacts/s, <queries> queries/s, in <inbytesps> B/s, out <outbytesps> B/s, xact <xactus> us, query <queryus> us, wait <waitus> us" | unwrap inbytesps [1m]))

sum by (target) (sum_over_time({logtype="pgbouncer"} |~ "LOG stats" | pattern "<_> <_> stats: <xacts> xacts/s, <queries> queries/s, in <inbytesps> B/s, out <outbytesps> B/s, xact <xactus> us, query <queryus> us, wait <waitus> us" | unwrap queries [1m]))

{target=~"video.+", logtype="postgres"} | pattern "[<_>] <level>:"
```