import re
from nltk.corpus import stopwords ##importing nltk packages
import json                       ##importing jason package
import time
##import Worker                     ##importing necessary directories
##import worker1 
import socket
timeList = [] ## storing time taken for each file in an array.

for i in range(1, 12): ## using iterations opening each file.
    with open('d'+str(i)+'_output.json', 'r') as f: 
        dataset = json.load(f)
    data = dataset["data"] ## taking only values stored in “data” attribute
    lines = len(data) ## length of data
    wrds = []
    start = time.time() 
    dct = dict()
    for j in range(lines): ## iterating all lines
        wrds += data[str(j)] ## storing all words in an array
    s = set(wrds) 
    for w in s:
        dct[w] = wrds.count(w)
    end = time.time()
    timeList.append(round(end-start, 3)) 
    print(len(dct))

for i in range(11):
    print("Doc:", i+1, "==>", timeList[i]) ## printing the time taken for each file
