#!/usr/bin/python

fileName="sls-nodes.json"
f=open(fileName, "w")
f.write("{\n\"rack\" : \"default-rack\",\n\"nodes\" : [ {\n")
for i in range (0,998):
    f.write("\"node\" : \"193.10.64.85:" + str(i) + "\"\n}, {\n")
f.write("\"node\" : \"193.10.64.85:" + str(999) + "\"\n} ]\n}")
