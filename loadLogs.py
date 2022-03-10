import json
import datetime
import pytz
import requests
import glob
import argparse
import os
import re
import sys

batchsize=200

def parse_postgres(line):
    try:
        order = ["date", "time", "tz", "process", "type"]
        details = line.split(" ")
        details = [x.strip() for x in details]
        structure = {key:value for key, value in zip(order, details)}
        # structure.update({"linenbr": linenbr})
        fulldate = datetime.datetime.strptime(structure.get("date") + " " + structure.get("time") + " " + structure.get("tz"), '%Y-%m-%d %H:%M:%S.%f %Z')
        structure.update({"ts": fulldate.replace(tzinfo=pytz.timezone(structure.get("tz"))).isoformat()})
        # structure.update({"line": line[line.find(structure.get("type"))+len(structure.get("type"))+2:]})
        structure.update({"line": line[line.find(structure.get("process")):]})
        structure.pop("process")
        structure.pop("tz")
        structure.pop("time")
        structure.pop("date")
        structure.pop("type")
        return structure
    except Exception:
        print("Error in File:", line)

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
        
def read_file(target, logtype, fn):
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
                structure = parse_postgres(line)
                data.append(structure)
                queuelines += 1
        
        if (logtype == "pgo"):
            structure = parse_pgo(line)
            data.append(structure)
            queuelines += 1

        if (logtype == "pgbouncer"):
            if re.match("^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",line):
                structure = parse_postgres(line)
                data.append(structure)
                queuelines += 1
                
        
        if (queuelines >= batchsize):
            loki_post(target, logtype, data)
            date = []
            queuelines=0
            
        linenbr += 1

    if queuelines > 0:
        loki_post(target, logtype, data)
        
    sys.stdout.write("Reading File %s   Lines Processed:  %d    " % (os.path.basename(fn), linenbr) )
    sys.stdout.flush()
    return linenbr

def loki_post(target, logtype, data):
    payload = {}
    payload.update({"streams": [{ "labels": "{target=\""+target+"\", logtype=\""+logtype+"\"}", "entries": data}]})
    r = requests.post('http://localhost:3100/api/prom/push',json=payload)
    if r.status_code != 204:
        print(r, r.text)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--dir", required=True)
    ap.add_argument("-t", "--type", required=False, default="unknown")
    args = vars(ap.parse_args())

    dirname = args['dir']
    logtype = args['type']

    for fn in glob.iglob(dirname+"/**/*.log", recursive=True):
        if logtype == "unknown":
            if fn.upper().find("OPERATOR.LOG") > 0:
                logtype = "pgo"
            elif fn.upper().find("POSTGRESQL") > 0:
                logtype = "postgres"
            elif fn.upper().find("PGBOUNCER") > 0:
                logtype = "pgbouncer"
            else:
                print("ERROR:  Cannot identify log type and none specified in command line arguement (-t)")
                exit()
        
        print("Identified Log Type as ", logtype)    
        target = fn[len(dirname)+1:].split("/")[0]
        lr = read_file(target, logtype, fn)
        print(" ")
                
        # # Python3 code to demonstrate
        # # converting comma separated string
        # # into dictionary

        # # Initialising string
        # ini_string1 = 'name = akshat, course = btech, branch = computer'

        # # Printing initial string
        # print ("Initial String", ini_string1)

        # # Converting string into dictionary
        # # using dict comprehension
        # res = dict(item.split("=") for item in ini_string1.split(", "))
                
        # # Printing resultant string
        # print ("Resultant dictionary", str(res))
            

        # loki_post(target, data)

if __name__ == '__main__':
    main()

