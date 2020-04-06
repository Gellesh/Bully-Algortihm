import sys
import re
import zmq
import time
import pickle
from datetime import datetime

def Read_config():
    fileName = 'config.txt'
    lineList = [line.rstrip('\n') for line in open(fileName)]

    number_of_machines = int(lineList[0])
    time_waited = int(lineList[1])
    coordinator_number = int(lineList[2])

    ports = []
    priorties = []
    machinesdata = dict()
    for i in range(3,3+number_of_machines):
        priorty , porttask , portpush , portpull =  re.split(" ",lineList[i])
    
        machinesdata[int(priorty)] = {'porttask':porttask,'portpush':portpush,'portpull':portpull}
    
    state = dict()
    
    # for i ,j in enumerate(range(3+number_of_machines , 3 + number_of_machines * 2 )):
    #     state[i+1] = lineList[j]

    return number_of_machines, machinesdata , time_waited , state ,coordinator_number

def updateconfig(new_coordinator):
    fileName = 'config.txt'
    lineList = [line for line in open(fileName)]
    lineList[2] = str(new_coordinator) + str("\n")
    #print(lineList)
    with open(fileName, 'w') as f2:
        f2.writelines(lineList)

def check_buffer(machinesdata,machineNo):
    # for pull sockets in election
    context = zmq.Context()
    socketpull = context.socket(zmq.PULL)
    socketpull.bind("tcp://127.0.0.1:%s" % machinesdata[machineNo]['portpull'])
    socketpull.RCVTIMEO = 0

    # for pull sockets in coordinator choose
    socketpush = context.socket(zmq.PULL)
    socketpush.bind("tcp://127.0.0.1:%s" % machinesdata[machineNo]['portpush'])
    socketpush.RCVTIMEO = 0

    # for task socket
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://127.0.0.1:%s" % str(machinesdata[machineNo]['porttask']))
    socket.RCVTIMEO = 0


    # empty pullports
    time.sleep(2)
    flag = 0
    while(flag > 100):
        try:
            message = socketpull.recv()
            message = pickle.loads(message)
            print(message['msg'],message['type'],message['id'])
                
        except zmq.error.Again:
            flag +=flag 
    socketpull.close(linger=100)
    time.sleep(1)

    # empty pushports
    flag = 0
    while(flag > 100):
        try:
            message = socketpush.recv()
            message = pickle.loads(message)
            print(message['msg'],message['type'],message['id'])
                
        except zmq.error.Again:
            flag +=flag 
    socketpush.close(linger=100)



def coordinator(machinesdata,id):
    print("Coordinator availabe Now !")
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % machinesdata[id]['porttask'])
    machines = set()
    while True:
        process_number = socket.recv()
        process_number = pickle.loads(process_number)
        msg_type = process_number['type']
        process_number = process_number['id']
        machines.add(process_number)
        #print("Recieved msg from client with id: %s  with type = %s" % (str(process_number),msg_type)) 

        if msg_type  == "doelection":
            msg = "process request doelection"
            t = time.time()
            print("send doelection request to machines : ",machines,"  time = ", t)
            coordinator_msg = {'type':"doelection",'id':machineNo,'msg': msg}
        
            coordinator_msg = pickle.dumps(coordinator_msg)
            for i in range(len(machines)):
                socket.send(coordinator_msg)
                if len(machines) == (i-1):
                    break
                msg = socket.recv()
                print(msg)
            break
            

        msg = "recieved ok msg from coordinator to client with id: %s" % str(process_number)
        coordinator_msg = {'type':"Coordinator response",'id':machineNo,'msg': msg,"cord_num":machineNo}
        
        coordinator_msg = pickle.dumps(coordinator_msg)
        socket.send(coordinator_msg)
    
    socket.close(linger = 1000)
    t = time.time()
    print("Coordinator is outing to do election ...","  time = ", t)


def processTask(machines, machineNo , oktime,coordinator_number,doelection = None):
    print("enter process")
    context = zmq.Context()
    socketclient = context.socket(zmq.REQ)
    socketclient.connect ("tcp://127.0.0.1:%s" % str(machines[coordinator_number]['porttask']))
    socketclient.RCVTIMEO = 1000 * oktime
    if doelection:
        msg_type = "doelection"
    else:
        msg_type = "normal process"
    while True:
        message = {'id':machineNo,'type':msg_type}
        message = pickle.dumps(message)
        socketclient.send(message)

        try:
            message = socketclient.recv()
            message = pickle.loads(message)
            #print(message['msg'],message['type'])
            if message['type'] =="doelection":
                t = time.time()
                print("process is outing to do election","  time = ", t)
                message = {'id':machineNo,'type':msg_type}
                message = pickle.dumps(message)
                socketclient.send(message)
                break

        except zmq.error.Again:
            #socketclient.close()
            t = time.time()
            print("time OUT","  time = ", t)
            break
               

    #flag = election(machineNo,machines,message['id'],oktime)
    print("out processs")
    socketclient.close(linger=1000)
    return False 


def election(machineNo,machines,last_dead,oktime):
    flag = False
    context = zmq.Context()
    socketpush = context.socket(zmq.PUSH)
    socketpull = context.socket(zmq.PULL)
    #socketpull.setsockopt(zmq.LINGER, 1000)
    socketpull.bind("tcp://127.0.0.1:%s" % machinesdata[machineNo]['portpull'])
    
    # send msg to higher machines
    for p , data in machines.items():
        if  p > machineNo:
            socketpush = context.socket(zmq.PUSH)
            socketpush.connect("tcp://127.0.0.1:%s" % data['portpull'])
            msg = "election request from lower p machine" + data['portpull']
            t = time.time()
            print(machineNo," bind to port pull ",p,data['portpull'] , msg,"  time = ", t)

            election_msg = {'type':"election request",'id':machineNo,'msg': msg}
            election_msg = pickle.dumps(election_msg)
            socketpush.send(election_msg)
            socketpush.close(linger=1000)
            time.sleep(0.1)
            
    # recieve msgs from pull socket
    t = time.time()
    print("connect to my port pull" + machinesdata[machineNo]['portpull'],"  time = ", t)
    
    socketpull.RCVTIMEO = 0
    current_time = datetime.timestamp(datetime.now())
    timer = 0
    while(timer < oktime):
        timer = datetime.timestamp(datetime.now()) - current_time
        try:
            message = socketpull.recv()
            message = pickle.loads(message)
            

            if message['type'] == "election request":  # lower machine send election to you
                t = time.time()
                print(message['msg'],message['type'],message['id'],"  time = ", t)
                socketpush = context.socket(zmq.PUSH)
                socketpush.connect("tcp://127.0.0.1:%s" % machinesdata[message['id']]['portpull'])
                msg = "ok message from higher machine  " + str(machineNo)
                ok_msg = {'type':"ok",'id':machineNo,'msg': msg}
                ok_msg = pickle.dumps(ok_msg)
                socketpush.send(ok_msg)
                socketpush.close()

            if message['type'] == 'ok':
                t = time.time()
                print(message['msg'],message['type'],message['id'],"  time = ", t)
                flag = True
                
        except zmq.error.Again:
            continue


    socketpull.close(linger=100)
    socketpush.close(linger=1000)
    time.sleep(0.1)

    print(machineNo,"finished loop ",flag)
    return flag
  

def check_coordinatorNumber(number_of_machines,machinesdata,ok_time):

    ifknow = False
    coordinator_number = machineNo
    for i  in range(number_of_machines):
        
        context = zmq.Context()
        socketclient = context.socket(zmq.REQ)
        print("bind to machine %d  to check if coordinator " %(i+1))
        socketclient.connect ("tcp://127.0.0.1:%s" % str(machinesdata[i+1]['porttask']))
        socketclient.RCVTIMEO = 1000 * ok_time
        
        while True:
            message = {'id':machineNo,'type':"identify"}
            message = pickle.dumps(message)
            socketclient.send(message)

            try:
                message = socketclient.recv()
                message = pickle.loads(message)
                print(message['cord_num'])
                coordinator_number = message['cord_num']
                ifknow = True
                break

            except zmq.error.Again:
                #socketclient.close()
                t = time.time()
                print("time OUT","  time = ", t)
                break
        
        socketclient.close(linger=0)
        print("leaving current machine")

        if ifknow:
            print("Cordinatoor number know : ",coordinator_number)
            break
        else:
            coordinator_number = machineNo

    return coordinator_number


### main ##
#time.sleep(2)
machineNo = int(sys.argv[1])
number_of_machines,machinesdata,ok_time,state,coordinator_number = Read_config()
print("entering buffer")
check_buffer(machinesdata,machineNo)
print("leaving buffer")

coordinator_number = check_coordinatorNumber(number_of_machines,machinesdata,ok_time)

flag2 = False
flag = False



if machineNo > coordinator_number:
    
    # added code #
    processTask(machinesdata, machineNo , ok_time,coordinator_number,True)

elif machineNo == coordinator_number:

    coordinator(machinesdata,machineNo)
        
else:
    flag  = processTask(machinesdata, machineNo, ok_time,coordinator_number)
    #state[coordinator_number] = 'dead'
    #updateconfig(state,number_of_machines)

while(True):
    
    flag = election(machineNo,machinesdata,state,ok_time)
    print("after election")
    if flag:

        flag2 = False
       
        context = zmq.Context()
        socketpull_coordinator = context.socket(zmq.PULL)
        socketpull_coordinator.bind("tcp://127.0.0.1:%s" % machinesdata[machineNo]['portpush'])
        socketpull_coordinator.RCVTIMEO = 0
        t = time.time()
        print("bind my port number waiting for Coordinator" + machinesdata[machineNo]['portpush'],"  time = ", t)
        current_time = datetime.timestamp(datetime.now())
        timer = 0
        while(timer < ok_time):
            timer = datetime.timestamp(datetime.now()) - current_time
            try:
                message = socketpull_coordinator.recv()
                message = pickle.loads(message)
                coordinator_number = message['id']
                t = time.time()
                print(message['msg'],message['type'],message['id'],"  time = ", t)  # here you know new coordinator
                flag2 = True
                break

            except zmq.error.Again:
                continue
                    
        socketpull_coordinator.close(linger=100)
        if flag2:
             flag = processTask(machinesdata, machineNo, ok_time,coordinator_number)
             #state[coordinator_number] = 'dead'
             #updateconfig(state,number_of_machines)
        else:
            print("error")
            

    else:
        context = zmq.Context()
        socketpush = context.socket(zmq.PUSH)
        # send msg to higher machines
        for p , data in machinesdata.items():
            if p != machineNo and p < machineNo:
                socketpush = context.socket(zmq.PUSH)
                socketpush.connect("tcp://127.0.0.1:%s" % data['portpush'])
                msg = "Coordinator Elected"
                election_msg = {'type':"Coordinator",'id':machineNo,'msg': msg}
                election_msg = pickle.dumps(election_msg)
                socketpush.send(election_msg)
                t  = time.time()
                print("send " + msg + "to port "+ data['portpush'],"  time = ", t)
                socketpush.close()
                #time.sleep(0.1)
        updateconfig(machineNo)            
        coordinator(machinesdata,machineNo)
        
        
        
    
print("end")