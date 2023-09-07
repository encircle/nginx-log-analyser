import re,os
import logging as log
from operator import itemgetter
from datetime import datetime



og_log_format='$http_x_forwarded_for $remote_addr $remote_user [$time_local] $request $status ' \
    '$body_bytes_sent "$http_referer" "$http_user_agent" $scheme$host$request_uri$request_method ' \
    'upstream_status=$upstream_status request_time=$request_time upstream_response_time=$upstream_response_time' \
    'upstream_connect_time=$upstream_connect_time upstream_header_time=$upstream_header_time'

log_format = '^(?P<http_x_forwarded_for>([\d.\w:,]+)\, ([\d.\w:]+)) (?P<remote_addr>[\d.]+) (?P<remote_user>[\S.]+) ' \
    '\[(?P<local_time>.+)\] "(?P<method>[A-Z]+) (?P<request>[\w\.\-\/]+).+" ' \
    '(?P<status>\d{3}) (?P<bytes_sent>\d+)' \
    '.*request_time=(?P<request_time>[\d\.]+|\-) '

def parsedir(filepath):
    results=[]
    for f in os.listdir(filepath):
        if 'access.log' in f and not f.endswith(".gz"):
            print(f"processing file {filepath}/{f}")
            results+=parsefile(filepath+"/"+f)
    return results


def parsefile(filepath):
    requests = []
    with open(filepath) as fl:
        for x, line in enumerate(fl):
            try:
                res=re.match(log_format,line)
                parsed_line = res.groupdict()
                requests.append(parsed_line)
                params=parsed_line
                params["status"]=int(params["status"])
                req_time=params["request_time"]
                if req_time=='-':
                    req_time=0.0
                else:
                    req_time= float(req_time)
                params["request_time"] = req_time
                local_time=params['local_time'].split(' +')[0]
                dt=datetime.strptime(local_time,'%d/%b/%Y:%H:%M:%S')
                params['local_timestamp']=dt
                params["line_no"] = x+1
            except Exception as e:
                print(line)
                log.error(f"could not parse line {x+1} of file {filepath}")


            pass

    return requests

os.makedirs("./reports",exist_ok=True)
requests=parsedir("nginx")

slow_requests = sorted(requests, key=itemgetter('request_time'),reverse=True)

total_requests=len(slow_requests)

under_01s=0
under_02s=0
under_05s=0
under_10s=0
under_20s=0
under_40s=0
under_80s=0

with open("./reports/response-times-individual.csv","w") as sl:
    sl.write("Request time in seconds,url,method,status,time\n")
    for req in slow_requests:
        req_time=req['request_time']
        sl.write(f"{req_time},{req['request']},{req['method']},{req['status']},{req['local_timestamp'].isoformat()}\n")
        if(req_time==0.0):
            pass
        elif(req_time<1.0):
            under_01s+=1
        elif(req_time<2.0):
            under_02s+=1
        elif(req_time<5.0):
            under_05s+=1
        elif(req_time<10.0):
            under_10s+=1
        elif(req_time<20.0):
            under_20s+=1
        elif(req_time<40.0):
            under_40s+=1
        elif(req_time<80.0):
            under_80s+=1

under_01s=(total_requests-under_01s)/total_requests
under_02s=(total_requests-under_02s)/total_requests
under_05s=(total_requests-under_05s)/total_requests
under_10s=(total_requests-under_10s)/total_requests
under_20s=(total_requests-under_20s)/total_requests
under_40s=(total_requests-under_40s)/total_requests
under_80s=(total_requests-under_80s)/total_requests

with open("./reports/statistics.csv","w") as fst:

    fst.write(f"total requests in range,{total_requests}\n")
    fst.write(f"percent requests under 1s,{under_01s*100.0:.3f}%\n")
    fst.write(f"percent requests under 2s,{under_02s*100.0:.3f}%\n")
    fst.write(f"percent requests under 5s,{under_05s*100.0:.3f}%\n")
    fst.write(f"percent requests under 10s,{under_10s*100.0:.3f}\n%")
    fst.write(f"percent requests under 20s,{under_20s*100.0:.3f}\n%")
    fst.write(f"percent requests under 40s,{under_40s*100.0:.3f}\n%")
    fst.write(f"percent requests under 80s,{under_80s*100.0:.3f}\n%")
    fst.write(f"percent requests under 80s,{under_80s*100.0:.3f}\n\n%")

    error_count=0
    for req in requests:
        req_status=req['status']
        if req_status>=500 and req_status<600:
            error_count+=1
    fst.write(f"errored request total,{error_count}\n")
    fst.write(f"errored request percent,{error_count/total_requests*100.0:.3f}%\n\n")
    for req in requests:
        req_status=req['status']
        if req_status>=500 and req_status<600:
            fst.write(f"{req['request']},{req_status},{req['local_timestamp'].isoformat()},")
            fst.write("\n")

response_dict={}

for req in slow_requests:
    url=req['request']
    if not url in response_dict:
        response_element={
            "url":url,
            "max_time":0.0,
            "min_time":-1.0,
            "total":0,
            "cumulative_time":0,
            "error_count": 0
        }
        response_dict[url]=response_element

    response_element=response_dict[url]
    response_time = req['request_time']

    max=response_element['max_time']
    if response_time>max:
        response_element['max_time']=response_time

    min=response_element['min_time']
    if response_time<min or min<0.0:
        response_element['min_time']=response_time

    response_element['total']+=1
    response_element['cumulative_time']+=response_time
    status=req['status']
    if(status>=500 and status<600):
        response_element['error_count'] += 1

response_element_list=list(response_dict.values())
response_element_list=sorted(response_element_list, key=itemgetter('cumulative_time'),reverse=True)

with open("./reports/response-times-aggregated.csv","w") as frsa:
    frsa.write("url,max_time,min_time,avg_time,cumulative_time,occurences,errors\n")
    for x in response_element_list:
        x["avg_time"]=x['cumulative_time']/x["total"]
        frsa.write(f"{x['url']},{x['max_time']},{x['min_time']},{x['avg_time']},{x['cumulative_time']},{x['total']},{x['error_count']}\n")
