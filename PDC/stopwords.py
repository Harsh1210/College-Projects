import re
from nltk.corpus import stopwords ##importing nltk packages
import json                       ##importing jason package
import time
##import Worker                     ##importing necessary directories
##import worker1 
import socket

stopWords = set(stopwords.words('english')) ## storing existing stopwords in set data structure.
specialChar = "~`!@#$%^&*()_+={}[]|\\:;\'\"<>?/.,"  
urlRE = re.compile("http")
fileName = "Dataset/d" ## location of file
for i in range(1, 12): 
    dct = dict()
    c = 0
    with open(fileName+str(i)+".txt", errors="ignore") as f: ## opening files one by one
        for lines in f: ## loop for removing special character, whitespaces or other unnecessary data from the set.
            lines = lines.strip().split('|') 
            lines = lines[0].split()
            wrds = []
            for phrase in  lines: 
                phrase = phrase.lower()
                phrase = phrase.rstrip("'s") 
                chars = list(phrase)
                phrase = ""
                for ch in chars:
                    if ch not in specialChar: 
                        phrase+=ch
                if urlRE.match(phrase) == None and phrase!="-" and phrase not in stopWords and phrase not in specialChar:
                    try:
                        phrase.encode('ascii') 
                        if phrase!="":
                            wrds.append(phrase)
                    except:
                        continue 
            if wrds!=[]:
                dct.update({c: wrds}) 
                c+=1
    data = {"data": dct}
    with open("d"+str(i)+"_output.json", 'w') as outfile: ## creating a new json file with key-value pairs of keys as numbers and values as stopwords of each tweet.
        json.dump(data, outfile)
