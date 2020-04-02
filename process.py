import sys
import re


def Read_config():
    fileName = 'config.txt'
    lineList = [line.rstrip('\n') for line in open(fileName)]

    number_of_machines = int(lineList[0])
    time_waited = int(lineList[1])

    ports = []
    priorties = []
    machinesdata = dict()
    for i in range(2,2+number_of_machines):
        priorty , port =  re.split(" ",lineList[i])
        ports.append(port)
        priorties.append(priorty)
        machinesdata[priorty] = port


    #print(number_of_machines , time_waited)
    #print(ports ,priorties)
    return number_of_machines, machinesdata , time_waited


input = sys.argv[1]
number_of_machines,data,time_waited = Read_config()
