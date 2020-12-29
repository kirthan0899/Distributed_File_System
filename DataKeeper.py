import zmq
import sys
import multiprocessing
import Conf
import time
import os
    
class DataKeeper(multiprocessing.Process):

    def __readConfiguration(self):
        self.__IP = Conf.DATA_KEEPER_IPs[self.__ID]
        self.__masterPort = Conf.DATA_KEEPER_MASTER_PORTs[self.__PID]
        self.__successPort = Conf.DATA_KEEPER_SUCCESS_PORTs[self.__PID]
        self.__downloadPort = Conf.DATA_KEEPER_DOWNLOAD_PORTs[self.__PID]
        self.__uploadPort = Conf.DATA_KEEPER_UPLOAD_PORTs[self.__PID]
        self.__replicaPort = Conf.DATA_KEEPER_REPLICA_PORTs[self.__PID]

    def __initConnection(self):
        context = zmq.Context()
        self.__masterSocket = context.socket(zmq.REP)
        self.__masterSocket.bind("tcp://%s:%s" % (self.__IP, self.__masterPort))
        self.__successSocket = context.socket(zmq.PUSH)
        self.__successSocket.bind("tcp://%s:%s" % (self.__IP, self.__successPort))
        self.__downloadSocket = context.socket(zmq.PUSH)
        self.__downloadSocket.bind("tcp://%s:%s" % (self.__IP, self.__downloadPort))
        self.__uploadSocket = context.socket(zmq.PULL)
        self.__uploadSocket.bind("tcp://%s:%s" % (self.__IP, self.__uploadPort))
        self.__replicaSendSocket = context.socket(zmq.REP)
        self.__replicaSendSocket.bind("tcp://%s:%s" % (self.__IP, self.__replicaPort))
        self.__replicaRcvSocket = context.socket(zmq.REQ)
        self.__poller = zmq.Poller()
        self.__poller.register(self.__masterSocket, zmq.POLLIN)
        self.__poller.register(self.__uploadSocket, zmq.POLLIN)

    def __init__(self, ID, PID):
        multiprocessing.Process.__init__(self)
        self.__ID = ID
        self.__PID = PID
        self.__readConfiguration()
    
    def run (self):
        self.__initConnection()
        while True:
            socks = dict(self.__poller.poll(5))
            if self.__masterSocket in socks and socks[self.__masterSocket] == zmq.POLLIN:
                self.__receiveRequestsFromMaster()
            elif self.__uploadSocket in socks and socks[self.__uploadSocket] == zmq.POLLIN:
                self.__receiveUploadRequest()
                
    def __receiveRequestsFromMaster(self):
        msg = self.__masterSocket.recv_json()
        print("msg received in DK")
        
        if msg['requestType'] == 'download':
            self.__receiveDownloadRequest(msg)
        elif msg['requestType'] == 'replicaSrc':
            self.__receiveReplicaSrcRequest(msg)
        elif msg['requestType'] == 'replicaDst':
            self.__receiveReplicaDstRequest(msg)
    
    def __receiveUploadRequest(self):
        rec = self.__uploadSocket.recv_pyobj()
        print("video rec")
        filePath = "data/DataKeeper/{}/{}".format(self.__ID, rec['fileName'])
        with open(filePath, "wb") as out_file:  # open for [w]riting as [b]inary
                out_file.write(rec['file'])
        
        successMessage = {'fileName': rec['fileName'], 'clientID': rec['clientID'], 'nodeID': self.__ID, 'processID': self.__PID }
        self.__successSocket.send_json(successMessage)
        print("done")

    def __receiveDownloadRequest(self, msg):
        if msg['mode'] == 1:
            chunksize = Conf.CHUNK_SIZE
            fileName = msg['fileName']
            filePath = "data/DataKeeper/{}/{}".format(self.__ID, fileName)
            size = os.stat(filePath).st_size
            size = (size+chunksize-1)//chunksize
            self.__masterSocket.send_pyobj({'size': size})
        else:
            MOD = msg['MOD']
            m = msg['m']  
            fileName = msg['fileName']
            self.__masterSocket.send_pyobj({'IP': self.__IP, 'PORT' : self.__downloadPort})
            self.__downloadToClient(MOD, m, fileName)

    def __downloadToClient(self, MOD, m, fileName):
        print("downloading to client")
        chunksize = Conf.CHUNK_SIZE
        filePath = "data/DataKeeper/{}/{}".format(self.__ID, fileName)
        size = os.stat(filePath).st_size
        sz = size
        size = (size+chunksize-1)//chunksize
        NumberofChuncks = size//MOD + (size%MOD >= m)
        with open(filePath, "rb") as file:
            for i in range(NumberofChuncks):
                step = chunksize*i*MOD + chunksize*m
                if step >= sz:
                    break
                file.seek(step)
                if step+chunksize > sz:
                    chunksize = sz - step
                chunk = file.read(chunksize)
                chunkNumber = (i*MOD+m).to_bytes(4,"big")
                self.__downloadSocket.send_multipart([chunkNumber,chunk])
        successMessage = {'fileName': fileName, 'clientID': -1, 'nodeID': self.__ID, 'processID': self.__PID }
        self.__successSocket.send_json(successMessage)


    def __receiveReplicaSrcRequest(self, msg):
        print("received replica src request")
        self.__masterSocket.send_json({'IP': self.__IP, 'PORT' : self.__replicaPort})
        filePath = "data/DataKeeper/{}/{}".format(self.__ID, msg['fileName'])

        print("sending file to dst from src")
        self.__replicaSendSocket.recv_string()
        with open(filePath, "rb") as file:
            self.__replicaSendSocket.send(file.read())
        print ("file is sent to data keeper")
        successMessage = {
            'fileName': msg['fileName'],
            'clientID': -1,
            'nodeID': self.__ID,
            'processID': self.__PID
        }
        self.__successSocket.send_json(successMessage)

    def __receiveReplicaDstRequest(self, msg):
        print("received replica dst request")
        self.__masterSocket.send_string("received src node")
        srcNode = msg['srcNode']
        filePath = "data/DataKeeper/{}/{}".format(self.__ID, msg['fileName'])
        print("dst connecting to {}".format(srcNode))
        self.__replicaRcvSocket.connect("tcp://%s:%s" % (srcNode['IP'], srcNode['PORT']))
        print("dst connected and receiving")
        self.__replicaRcvSocket.send_string("send file")
        file = self.__replicaRcvSocket.recv()
        self.__replicaRcvSocket.disconnect("tcp://%s:%s" % (srcNode['IP'], srcNode['PORT']))
        print("dst disconnected from {}".format(srcNode))

        with open(filePath, "wb") as out_file:
            out_file.write(file)
            
        successMessage = {
            'fileName': msg['fileName'],
            'clientID': self.__ID,
            'nodeID': self.__ID,
            'processID': self.__PID
        }
        self.__successSocket.send_json(successMessage)

    def __heartBeatsConfiguration(self):
        context = zmq.Context()
        self.__aliveSocket = context.socket(zmq.PUB)
        self.__aliveSocket.bind("tcp://%s:%s"%(self.__IP, Conf.ALIVE_PORT))

    def heartBeats(self):
        self.__heartBeatsConfiguration()
        while True:
            self.__aliveSocket.send_string("%d" % (self.__ID))
            time.sleep(1)
    

if __name__ == '__main__':
    
    manager = multiprocessing.Manager()
    
    ID = int(sys.argv[1])
    numOfProcesses = len(Conf.DATA_KEEPER_MASTER_PORTs)
    processes = []
    for i in range(numOfProcesses):
        processes.append(DataKeeper(ID, i))
        processes[i].start()

    heartBeatsProcess = multiprocessing.Process(target=processes[0].heartBeats)
    heartBeatsProcess.start()

    for i in range(numOfProcesses):
        processes[i].join()
    heartBeatsProcess.join()

