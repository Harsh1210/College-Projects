import re
from nltk.corpus import stopwords   ##importing nltk packages
import json                         ##importing jason package
import time
##import Worker                     ##mporting necessary directories
##import worker1

import socket
import worker
# The data source can be any dictionary-like object
#datasource = dict(enumerate(data))

def mapfn(k, v): ## function for mapping
    for w in v:
        yield w, 1

def reducefn(k, vs): ## function for reducing
    result = sum(vs)
    return result

s = worker.Server() ## Connecting the Server
s.mapfn = mapfn
s.reducefn = reducefn

timeList = []

for i in range(1, 8):
    with open('d'+str(i)+'_output.json', 'r') as f:
        dataset = json.load(f)
    _datasource = dataset['data']
    s._datasource = _datasource
    start = time.time()
    results = s.run_server(password="scorpion11")
    end = time.time()
    timeList.append(round(end-start, 3))
    print(len(results))
    with open('MR_Output/d'+str(i)+'_ansOfMapReduce.txt', 'w') as f:
        for keys in sorted(results):
            line = keys+"\t\t\t"+str(results[keys])+"\n"
            f.write(line)

for i in range(1, 8):
    print("Doc:", i, timeList[i-1])

#%%


