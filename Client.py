import zmq
import sys
import Conf
 
class Client:

    def __initConnection(self):
        self.__context = zmq.Context()
        self.__masterSocket = self.__context.socket(zmq.REQ)
        for port in Conf.MASTER_CLIENT_PORTs:
            self.__masterSocket.connect("tcp://%s:%s" % (Conf.MASTER_IP, port))

    def __init__(self, ID):
        self.__ID = ID
        self.__initConnection()

    def __downloadFromNode(self, filePath, masterMsg):
        print("downloading from data keepers")
        addresses = masterMsg['freeports']
        numberOfChunks = masterMsg['numberofchunks']
        downSocket = self.__context.socket(zmq.PULL)
        for address in addresses:
            downSocket.connect("tcp://%s:%s"%(address['IP'], address['PORT']))
        data = []
        while numberOfChunks > 0:
            msg = downSocket.recv_multipart()
            chunckNumber = int.from_bytes(msg[0], "big")
            decodedmsg = {'chunckNumber' : chunckNumber, 'data': msg[1]}
            data.append(decodedmsg)
            numberOfChunks -= 1
        data.sort(key=lambda i: i['chunckNumber'])
        
        with open(filePath, "wb") as file:
            for i in data:
                file.write(i['data'])

        for address in addresses:
            downSocket.disconnect("tcp://%s:%s"%(address['IP'], address['PORT']))
        downSocket.close()
        return 'successful download'
        
    def sendUploadRequest(self, fileName, filePath):
        with open(filePath, "rb") as file:
            toSend = {'fileName':fileName, 'file':file.read(), 'clientID': self.__ID}
            msg = {'requestType': 'upload', 'file': fileName } 
            print("video fetched")
            self.__masterSocket.send_json(msg)
            print ("msg sent")
            freePort = self.__masterSocket.recv_json()
            print("msg returned")

            socket_upload = self.__context.socket(zmq.PUSH)
            socket_upload.connect("tcp://%s:%s"%(freePort['IP'], freePort['PORT'])) 
            socket_upload.send_pyobj(toSend)

            socket_upload.disconnect("tcp://%s:%s"%(freePort['IP'], freePort['PORT']))
            socket_upload.close()

    def sendDownloadRequest(self, fileName, filePath):
        requestMsg = {'requestType': 'download', 'file': fileName}
        self.__masterSocket.send_json(requestMsg)
        # print("download request sent from client to master")
        responseMsg = self.__masterSocket.recv_json()
        print("download response received from master to client:\n{}".format(responseMsg))
        if 'freeports'not in responseMsg or len(responseMsg['freeports']) == 0:
            msg = 'Failed to download'
            if ('message' in responseMsg):
                msg += ': '+ responseMsg["message"]
            return msg
        return self.__downloadFromNode(filePath, responseMsg)

if __name__ == '__main__':
    
    ID = int(sys.argv[1])
    cl1 = Client(ID)

    while True:
        command = str.lower(input("Enter command [upload, download, exit]: "))
        if command == 'exit':
            break
        elif command == 'upload':
            fileName = input("Enter filename: ")
            filePath = input("Enter filePath: ")
            cl1.sendUploadRequest(fileName, filePath)
        elif command == 'download':
            fileName = input("Enter filename: ")
            filePath = input("Enter filePath: ")
            print(cl1.sendDownloadRequest(fileName, filePath))
        else:
            print("%s: command not found" % (command))

