import zmq
import sys
import json
import multiprocessing
import Conf
import time
import datetime

class Master(multiprocessing.Process):

    def __readConfiguration(self):
        self.__IP = Conf.MASTER_IP
        self.__clientPort = Conf.MASTER_CLIENT_PORTs[self.__PID]

    def __initConnection(self):
        context = zmq.Context()
        self.__successSocket = context.socket(zmq.PULL)
        self.__clientSocket = context.socket(zmq.REP)
        self.__clientSocket.bind("tcp://%s:%s" % (self.__IP, self.__clientPort))
        self.__dataKeeperSocket = context.socket(zmq.REQ)
        for dKIP in Conf.DATA_KEEPER_IPs:
            for dKPort in Conf.DATA_KEEPER_SUCCESS_PORTs:
                self.__successSocket.connect("tcp://%s:%s" % (dKIP, dKPort))

        self.__poller = zmq.Poller()
        self.__poller.register(self.__successSocket, zmq.POLLIN)
        self.__poller.register(self.__clientSocket, zmq.POLLIN)


    def __init__(self, PID):
        multiprocessing.Process.__init__(self)
        self.__PID = PID
        self.__readConfiguration()
        
    def __initReplicaConnection(self):
        context = zmq.Context()
        self.__dataKeeperSocket = context.socket(zmq.REQ)


    def makeReplicates(self):
        self.__initReplicaConnection()
        while True:
            for file in iter(filesTable.keys()):
                cnt = 0
                srcNode = -1
                for node in filesTable[file]['nodes']:
                    if aliveTable[node]['isAlive']:
                        cnt += 1
                        if srcNode == -1:
                            srcNode = node
                if srcNode == -1:
                    continue
                while cnt < 3:
                    # print(file, cnt)
                    dstNode = -1
                    for node in range(len(aliveTable)):
                        if aliveTable[node]['isAlive'] and (node not in filesTable[file]['nodes']):
                            dstNode = node
                            break
                    if dstNode == -1:
                        break
                    print("srcNode = {}".format(srcNode))
                    print ("dstNode = {}".format(dstNode))
                    lock.acquire()
                    srcPID = 0
                    while usedPorts[srcNode][srcPID] == True:
                        srcPID = (srcPID+1)%len(Conf.DATA_KEEPER_MASTER_PORTs)
                    usedPorts[srcNode][srcPID] = True
                    dstPID = 0
                    while usedPorts[dstNode][dstPID] == True:
                        dstPID = (dstPID+1)%len(Conf.DATA_KEEPER_MASTER_PORTs)
                    usedPorts[dstNode][dstPID] = True
                    lock.release()
                    srcPort = {'IP': Conf.DATA_KEEPER_IPs[srcNode], 'PORT': Conf.DATA_KEEPER_MASTER_PORTs[srcPID]}
                    sendPort = self.__sendSrcReplicaRequest(srcPort, file)
                    print(sendPort)
                    dstPort = {'IP': Conf.DATA_KEEPER_IPs[dstNode], 'PORT': Conf.DATA_KEEPER_MASTER_PORTs[dstPID]}
                    self.__sendDstReplicaRequest(dstPort, sendPort, file)

                    while dstNode not in filesTable[file]['nodes']:
                        continue
                    cnt += 1
    
    def __sendSrcReplicaRequest(self, srcNode, fileName):
        msg = {'requestType':'replicaSrc', 'fileName': fileName}
        self.__dataKeeperSocket.connect("tcp://%s:%s" % (srcNode['IP'], srcNode['PORT']))
        self.__dataKeeperSocket.send_json(msg)
        port = self.__dataKeeperSocket.recv_json()
        self.__dataKeeperSocket.disconnect("tcp://%s:%s" % (srcNode['IP'], srcNode['PORT']))
        return port

    def __sendDstReplicaRequest(self, dstNode, srcNode, fileName):
        msg = {'requestType': 'replicaDst', 'srcNode': srcNode, 'fileName': fileName}
        self.__dataKeeperSocket.connect("tcp://%s:%s" % (dstNode['IP'], dstNode['PORT']))
        self.__dataKeeperSocket.send_json(msg)
        self.__dataKeeperSocket.recv_string()
        self.__dataKeeperSocket.disconnect("tcp://%s:%s" % (dstNode['IP'], dstNode['PORT']))
                
    def __checkSuccess(self):
        message = self.__successSocket.recv_json()
        if message['clientID'] != -1:
            lock.acquire()
            if message['fileName'] not in filesTable:
                filesTable[message['fileName']] = manager.dict({'clientID': message['clientID'], 'nodes': manager.list()})
            filesTable[message['fileName']]['nodes'].append(message['nodeID'])
            lock.release()
        usedPorts [message['nodeID']][message['processID']] = False
        print("success:", filesTable[message['fileName']]['nodes'])
        print(usedPorts[0], usedPorts[1], usedPorts[2])
        

    def __chooseUploadNode (self):
        numNodes = len(Conf.DATA_KEEPER_IPs)
        numProcessesInNodes = len(Conf.DATA_KEEPER_MASTER_PORTs)
        lock.acquire()
        currentNode = RRNodeItr.value
        currentProcess = RRProcessItr.value
        while usedPorts[RRNodeItr.value][RRProcessItr.value] == True or aliveTable[RRNodeItr.value]['isAlive'] == False:
            RRNodeItr.value +=1
            RRNodeItr.value %=numNodes
            if RRNodeItr.value == 0:
                RRProcessItr.value += 1
                RRProcessItr.value %= numProcessesInNodes
            if RRNodeItr.value == currentNode and RRProcessItr == currentProcess:
                break
        while aliveTable[RRNodeItr.value]['isAlive'] == False:
            RRNodeItr.value +=1
            RRNodeItr.value %=numNodes
            
        usedPorts[RRNodeItr.value][RRProcessItr.value] = True
        lock.release()
        freePort = {'IP': Conf.DATA_KEEPER_IPs[RRNodeItr.value] ,
         'PORT': Conf.DATA_KEEPER_UPLOAD_PORTs[RRProcessItr.value] }
        return freePort
            
    def __receiveUploadRequest(self):
        freePort = self.__chooseUploadNode() 
        self.__clientSocket.send_json(freePort)
        print("msg sent to client")

    def __receiveDownloadRequest(self, fileName):
        if fileName not in filesTable:
            self.__clientSocket.send_json({'message': "file not found"})
            return
        freePorts = []
        size = 0
        mxPorts = 4
        for i in filesTable[fileName]['nodes']:
            if aliveTable[i]['isAlive'] == False:
                continue
            Node_IP = Conf.DATA_KEEPER_IPs[i]
            for j in range(len(Conf.DATA_KEEPER_MASTER_PORTs)):
                if(len(freePorts) == mxPorts):
                    break
                lock.acquire()
                if(usedPorts[i][j] == False):
                    usedPorts[i][j] = True
                    lock.release()
                    Node_Port = Conf.DATA_KEEPER_MASTER_PORTs[j]
                    if len(freePorts) == 0:
                        msg = {'requestType': 'download', 'mode' : 1, 'fileName': fileName}
                        self.__dataKeeperSocket.connect("tcp://%s:%s" % (Node_IP, Node_Port))
                        self.__dataKeeperSocket.send_json(msg)
                        msg = self.__dataKeeperSocket.recv_pyobj()
                        size = msg['size']
                        # print("size of requested file = {}".format(size))
                        self.__dataKeeperSocket.disconnect("tcp://%s:%s" % (Node_IP, Node_Port))
                    freePorts.append({'Node': Node_IP, 'Port': Node_Port})
                else:
                    lock.release()
        MOD = len(freePorts)
        j = 0
        downloadPorts = []
        for i in freePorts:
            self.__dataKeeperSocket.connect("tcp://%s:%s" % (i['Node'], i['Port']))
            self.__dataKeeperSocket.send_json({'requestType': 'download', 'fileName': fileName, 'mode': 2, 'm': j, 'MOD': MOD})
            j += 1
            downloadPort = self.__dataKeeperSocket.recv_pyobj()
            self.__dataKeeperSocket.disconnect("tcp://%s:%s"% (i['Node'], i['Port']))
            downloadPorts.append(downloadPort)
        self.__clientSocket.send_json({'freeports': downloadPorts, 'numberofchunks': size})

    
    def __receiveRequestFromClient(self):
        message = self.__clientSocket.recv_json()
        print("msg got")
        if message['requestType'] == 'upload':
            self.__receiveUploadRequest()
        if message['requestType'] == 'download':
            self.__receiveDownloadRequest(message['file'])
              

    def run(self):
        self.__initConnection()
        while True:
            socks = dict(self.__poller.poll())
            if self.__successSocket in socks and socks[self.__successSocket] == zmq.POLLIN:
                self.__checkSuccess()
            elif self.__clientSocket in socks and socks[self.__clientSocket] == zmq.POLLIN:
                self.__receiveRequestFromClient()
    
    def __haertBeatsConfiguration(self):
        context = zmq.Context()
        self.__aliveSocket = context.socket(zmq.SUB)
        self.__aliveSocket.setsockopt_string(zmq.SUBSCRIBE, "")
        # connect to each data_node
        for dKIP in Conf.DATA_KEEPER_IPs:
            self.__aliveSocket.connect("tcp://%s:%s"%(dKIP, Conf.ALIVE_PORT))

        self.__alivePoller = zmq.Poller()
        self.__alivePoller.register(self.__aliveSocket, zmq.POLLIN)

    def heartBeats(self):
        self.__haertBeatsConfiguration()
        while True:
            socks = dict(self.__alivePoller.poll(1000))
            if socks:
                topic =  int ( self.__aliveSocket.recv_string(zmq.NOBLOCK) )
                aliveTable[topic]["lastTimeAlive"] = datetime.datetime.now()
                aliveTable[topic]["isAlive"] = True

            for i  in range(len(Conf.DATA_KEEPER_IPs)):

                if( (datetime.datetime.now() - aliveTable[i]["lastTimeAlive"]).seconds > 1):
                    aliveTable[i]["isAlive"] = False

if __name__ == '__main__':
    manager = multiprocessing.Manager()
    lock = multiprocessing.Lock()
    RRNodeItr = manager.Value('i',0) 
    RRProcessItr = manager.Value('i',0)
    usedPorts = manager.dict()
    aliveTable = manager.list()
    numNodes = len(Conf.DATA_KEEPER_IPs)
    numProcessesInNodes = len(Conf.DATA_KEEPER_MASTER_PORTs)
    for i in range(numNodes):
        listOfProcesses = manager.list()
        entry = manager.dict({"lastTimeAlive":datetime.datetime.now(),"isAlive": False})
        aliveTable.append(entry)
        for j in range (numProcessesInNodes):
            listOfProcesses.append(False)
        usedPorts[i] = listOfProcesses   
    filesTable = manager.dict()

    servers = []
    for i in range(len(Conf.MASTER_CLIENT_PORTs)):
        servers.append(Master(i))
        servers[i].start()
    
    aliveProcess = multiprocessing.Process(target=servers[0].heartBeats)
    aliveProcess.start()

    replicaProcess = multiprocessing.Process(target=servers[0].makeReplicates)
    replicaProcess.start()

    for i in range(len(Conf.MASTER_CLIENT_PORTs)):
        servers[i].join()
    replicaProcess.join()
    aliveProcess.join()
