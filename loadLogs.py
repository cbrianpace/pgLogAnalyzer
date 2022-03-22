import json
import datetime
import pytz
import requests
import glob
import argparse
import os
import re
import sys

batchsize=1000

def parse_postgres(line, dtformat):
    try:
        # Postgres Log Date %m = 2022-03-22 11:17:57.954 EDT
        # Postgres Log Date %t = 2022-03-22 11:41:10 EDT
        order = ["date", "time", "tz", "process", "type"]
        details = line.split(" ")
        details = [x.strip() for x in details]
        structure = {key:value for key, value in zip(order, details)}

        # Adjust for daylight savings time
        if (structure.get("tz").find("DT") > 0):
            tmptz = structure.get("tz")
            tmptz = tmptz[0:1]+"ST5"+tmptz
        else:
            tmptz = structure.get("tz")
            
        if (dtformat == "m"):
            fulldate = datetime.datetime.strptime(structure.get("date") + " " + structure.get("time") + " " + structure.get("tz"), '%Y-%m-%d %H:%M:%S.%f %Z')
        else:
            fulldate = datetime.datetime.strptime(structure.get("date") + " " + structure.get("time") + ".00000 " + structure.get("tz"), '%Y-%m-%d %H:%M:%S.%f %Z')
        
        structure.update({"ts": fulldate.replace(tzinfo=pytz.timezone(tmptz)).isoformat()})
        # structure.update({"line": line[line.find(structure.get("type"))+len(structure.get("type"))+2:]})
        structure.update({"line": line[line.find(structure.get("process")):]})
        structure.pop("process")
        structure.pop("tz")
        structure.pop("time")
        structure.pop("date")
        structure.pop("type")
        return structure
    except Exception as e:
        print("Error in File:", line)
        print(e)

def parse_pgo(line):
    try:
        structure ={}
        #test=r'time="2022-02-24T21:56:38Z" level=debug msg="replaced configuration" file="internal/patroni/api.go:86" func=patroni.Executor.ReplaceConfiguration name=testarossa-db namespace=pgo reconciler group=postgres-operator.crunchydata.com reconciler kind=PostgresCluster stderr= stdout="Not changed\n" version=5.0.4-0'
        # print(re.split(' (?=\[)',test))
        lineparse = re.findall(r'(?:[^\s,"]|"(?:\\.|[^"])*")+', line)
        structure.update({"ts": lineparse[0][6:].replace('"','').replace("Z",".00000+00:00")})
        structure.update({"line": line[len(lineparse[0])+1:]})
        return structure
    except Exception:
        print("Error in File:", line)

def parse_syslog(line, tz):
    try:
        structure ={}
        #Mar 22 08:22:23 jaxhippo01 systemd[1]: Started User Manager for UID 0.
        # print(re.split(' (?=\[)',test))
        tempdate = line[0:6] + " " + str(datetime.datetime.now().year) + " " + line[7:15] + ".00000" + tz
        fulldate = datetime.datetime.strptime(tempdate, '%b %d %Y %H:%M:%S.%f%z').isoformat()
        structure.update({"ts": fulldate})
        structure.update({"line": line[16:]})
        return structure
    except Exception as e:
        print("Error in File:", line)
        print(e)
        
def read_file(target, logtype, fn, tz, dtformat):
    linenbr = 1
    file = open(fn, "r")
    queuelines = 0
    data = []

    for line in file.readlines():
        sys.stdout.write("Reading File %s   Lines Processed:  %d   \r" % (os.path.basename(fn), linenbr) )
        sys.stdout.flush()
        structure = {}
                
        if (logtype == "postgres"):
            if re.match("^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",line):
                structure = parse_postgres(line, dtformat)
                data.append(structure)
                queuelines += 1
        
        if (logtype == "pgo"):
            structure = parse_pgo(line)
            data.append(structure)
            queuelines += 1

        if (logtype == "pgbouncer"):
            if re.match("^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",line):
                structure = parse_postgres(line, dtformat)
                data.append(structure)
                queuelines += 1
                
        if (logtype == "syslog"):
            structure = parse_syslog(line, tz)
            data.append(structure)
            queuelines += 1
                    
        if (queuelines >= batchsize):
            loki_post(target, logtype, os.path.basename(fn), data)
            data = []
            queuelines=0
            
        linenbr += 1

    if queuelines > 0:
        loki_post(target, logtype, os.path.basename(fn), data)
        
    sys.stdout.write("Reading File %s   Lines Processed:  %d    " % (os.path.basename(fn), linenbr) )
    sys.stdout.flush()
    return linenbr

def loki_post(target, logtype, fn, data):
    payload = {}
    payload.update({"streams": [{ "labels": "{target=\""+target+"\", logtype=\""+logtype+"\", filename=\""+fn+"\"}", "entries": data}]})
    r = requests.post('http://localhost:3100/api/prom/push',json=payload)
    if r.status_code != 204:
        print(r, r.text)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--dir", required=True)
    ap.add_argument("-t", "--type", required=False, default="unknown")
    ap.add_argument("-z", "--timezone", required=False, default="+00:00")
    ap.add_argument("-f", "--dateformat", required=False, default="m")
    args = vars(ap.parse_args())

    dirname = args['dir']
    logtypearg = args['type']
    tz = args['timezone']
    dtformat = args['dateformat']

    for fn in glob.iglob(dirname+"/**/*.log", recursive=True):
        if logtypearg == "unknown":
            if fn.upper().find("OPERATOR.LOG") > 0:
                logtype = "pgo"
            elif fn.upper().find("POSTGRESQL") > 0:
                logtype = "postgres"
            elif fn.upper().find("PGBOUNCER") > 0:
                logtype = "pgbouncer"
            else:
                print("ERROR:  Cannot identify log type:", os.path.basename(fn))
                continue
        else:
            logtype = logtypearg
        
        print("Identified Log Type as ", logtype)
        target = fn[len(dirname)+1:].split("/")[0]
        lr = read_file(target, logtype, fn, tz, dtformat)
        print(" ")

if __name__ == '__main__':
    main()

