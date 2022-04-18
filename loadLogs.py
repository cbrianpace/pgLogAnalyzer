import json
import datetime
import pytz
import requests
import glob
import argparse
import os
import re
import sys
import logging

batchsize=1000

logFormat = "%(asctime)s %(levelname)s - %(message)s"

########################################
## PARSE:  PostgreSQL Log
########################################
def parse_postgres(line, dtformat, lasttz):
    try:
        # Postgres Log Date %m = 2022-03-22 11:17:57.954 EDT
        # Postgres Log Date %t = 2022-03-22 11:41:10 EDT
        order = ["date", "time", "tz", "process", "type"]
        details = line.split(" ")
        details = [x.strip() for x in details]
        structure = {key:value for key, value in zip(order, details)}

        # Adjust for pgBackRest leaving timezone out of log   
        if (structure.get("tz")[0:2].find("P0") >= 0):
            structure.update({"tz": lasttz})
            structure.update({"line": line[line.find(" P0"):]})
            if (dtformat == "t"):
                structure.update({"time": structure.get("time")[0:8] })
        else:
            lasttz = structure.get("tz")
            structure.update({"line": line[line.find(structure.get("process")):]})

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
        structure.pop("process")
        structure.pop("tz")
        structure.pop("time")
        structure.pop("date")
        structure.pop("type")
        return structure, lasttz
    except Exception as e:
        app_message("Error in File () "+line, "warning", False)        

########################################
## PARSE:  database pod
########################################
def parse_pod_database(line, dtformat):
    try:
        # 2022-01-27 19:38:12,320 INFO: trying to bootstrap a new cluster
        order = ["date", "time"]
        details = line.split(" ")
        details = [x.strip() for x in details]
        structure = {key:value for key, value in zip(order, details)}

        fulldate = datetime.datetime.strptime(structure.get("date") + " " + structure.get("time").replace(",",".") + " UTC", '%Y-%m-%d %H:%M:%S.%f %Z')
        
        structure.update({"ts": fulldate.isoformat()})
        structure.update({"line": line[23:]})
        structure.pop("time")
        structure.pop("date")
        return structure
    except Exception as e:
       app_message("Error in File () "+line, "warning", False)

        
########################################
## PARSE:  pgo pod log
########################################
def parse_pgo(line):
    try:
        structure ={}
        #test=r'time="2022-02-24T21:56:38Z" level=debug msg="replaced configuration" file="internal/patroni/api.go:86" func=patroni.Executor.ReplaceConfiguration name=testarossa-db namespace=pgo reconciler group=postgres-operator.crunchydata.com reconciler kind=PostgresCluster stderr= stdout="Not changed\n" version=5.0.4-0'
        # print(re.split(' (?=\[)',test))
        lineparse = re.findall(r'(?:[^\s,"]|"(?:\\.|[^"])*")+', line)
        structure.update({"ts": lineparse[0][6:].replace('"','').replace("Z",".00000+00:00")})
        structure.update({"line": line[len(lineparse[0])+1:]})
        return structure
    except Exception as e:
        app_message("Error in File () "+line, "warning", False)

########################################
## PARSE:  exporter pod
########################################
def parse_exporter_pod(line):
    try:
        structure ={}
        #time="2022-01-27T19:38:33Z" level=info msg="Established new database connection to \"localhost:25432\"." source="postgres_exporter.go:878"
        lineparse = re.findall(r'(?:[^\s,"]|"(?:\\.|[^"])*")+', line)
        structure.update({"ts": lineparse[0][6:].replace('"','').replace("Z",".00000+00:00")})
        structure.update({"line": line[len(lineparse[0])+1:]})
        return structure
    except Exception as e:
        app_message("Error in File: "+line, "warning", False)


########################################
## PARSE:  syslog
########################################
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
        app_message("Error in File: "+line, "warning", False)

########################################
## Read File
########################################        
def read_file(target, logtype, fn, tz, dtformat, customer):
    linenbr = 1
    lineerrors = 0
    file = open(fn, "r")
    queuelines = 0
    data = []
    lasttz = "UTC"

    for line in file.readlines():
        try:
            sys.stdout.write("Reading File %s   Lines Processed:  %d   (%d errors)\r" % (os.path.basename(fn), linenbr, lineerrors) )
            sys.stdout.flush()
            structure = {}
                    
            if (logtype == "postgres"):
                if re.match("^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",line):
                    (structure, lasttz) = parse_postgres(line, dtformat, lasttz)
                    data.append(structure)
                    queuelines += 1
            
            if (logtype == "pod-pgo"):
                structure = parse_pgo(line)
                data.append(structure)
                queuelines += 1

            if (logtype == "pod-db"):
                if re.match("^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",line):
                    structure = parse_pod_database(line, dtformat)
                    data.append(structure)
                    queuelines += 1

            if (logtype == "pod-exporter"):
                if re.match("^\time",line):
                    structure = parse_exporter_pod(line, dtformat)
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
                loki_post(target, logtype, os.path.basename(fn), data, customer)
                data = []
                queuelines=0
                
            linenbr += 1
        except Exception as e:
            lineerrors += 1
            print(e)
            print(line)
            print(" ")
            app_message("Error in Reading Line "+line, "warning", False)
            
    if queuelines > 0:
        loki_post(target, logtype, os.path.basename(fn), data, customer)
        
    sys.stdout.write("Read File %s   Lines Processed:  %d    (%d errors)     \r" % (os.path.basename(fn), linenbr, lineerrors) )
    sys.stdout.flush()
    print("")
    return linenbr

########################################
## Post Payload to Loki
########################################
def loki_post(target, logtype, fn, data, customer):
    try:
        payload = {}
        payload.update({"streams": [{ "labels": "{customer=\""+customer+"\", target=\""+target+"\", logtype=\""+logtype+"\", filename=\""+fn+"\"}", "entries": data}]})
        r = requests.post('http://localhost:3100/api/prom/push',json=payload)
        if r.status_code != 204:
            app_message("Error in post to Loki "+r.text, "warning", False)
            # print(r, r.text)
            
    except Exception as e:
        lineerrors += 1
        app_message("Error in post to Loki "+line, "warning", False)

########################################
## App Message Logging
########################################
def app_message(message, level, toconsole):
    logger = logging.getLogger()
    
    if toconsole:
        print(message)
    
        if level == "info":
            logger.info(message)
            
        if level == "warning":
            logger.warning(message)
            
        if level == "error":
            logger.error(message)
         
        

########################################
## MAIN
########################################
def main():
    logging.basicConfig(filename = "load.log", filemode ="w", format = logFormat, level = logging.INFO)
    logger = logging.getLogger()
    
    logger.info("Starting log load")
    
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--dir", required=True)
    ap.add_argument("-t", "--type", required=False, default="unknown")
    ap.add_argument("-z", "--timezone", required=False, default="+00:00")
    ap.add_argument("-f", "--dateformat", required=False, default="m")
    ap.add_argument("-c", "--customer", required=False, default="unknown")
    args = vars(ap.parse_args())

    dirname = args['dir']
    logtypearg = args['type']
    tz = args['timezone']
    dtformat = args['dateformat']
    customer = args['customer']

    for fn in glob.iglob(dirname+"/**/*.log", recursive=True):
        if logtypearg == "unknown":
            if fn.upper().find("OPERATOR.LOG") > 0:
                logtype = "pod-pgo"
            elif fn.upper().find("DATABASE.LOG") > 0:
                logtype = "pod-db"
            elif fn.upper().find("POSTGRESQL") > 0:
                logtype = "postgres"
            elif fn.upper().find("PGBOUNCER.LOG") > 0:
                logtype = "pgbouncer"
            elif fn.upper().find("EXPORTER.LOG") > 0:
                logtype = "pod-exporter"
            else:
                app_message("ERROR:  Cannot identify log type: "+os.path.basename(fn), "error", True)
                continue
        else:
            logtype = logtypearg
        
        app_message("Identified Log Type as "+logtype, "info", True)
        target = fn[len(dirname)+1:].split("/")[0]
        lr = read_file(target, logtype, fn, tz, dtformat, customer)

if __name__ == '__main__':
    main()

