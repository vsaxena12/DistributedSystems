#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import glob
import sys
import hashlib
import os
import socket
import logging

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from chord.ttypes import NodeID, RFile, RFileMetadata, SystemException

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

# Other importent functionalities as per requirements
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Chord Server")

def fetchC_node():
    node = NodeID()
    node.ip = socket.gethostbyname(socket.getfqdn())
    node.port = int(sys.argv[1])
    node.id = hashlib.sha256(str(node.ip)+":"+str(node.port)).hexdigest()
    return node;

def customized_exception(error_message):
    myException = SystemException()
    myException.message = error_message
    return myException


class FileStoreHandler:
    #node = NodeID()
    #node.ip = socket.gethostbyname(socket.gethostname())
    #node.port = int(sys.argv[1])
    #node.id = hashlib.sha256(str(node.ip) + ":" + str(node.port)).hexdigest()
    node = fetchC_node()

    def __init__(self):
        self.log = {}
        self.node_list = []
        self.c_node = self.node

    def writeFile(self,rFile):
	logger.info("Write of File %s requested by Qwner %s: " % (rFile.meta.filename, rFile.meta.owner))
        self.c_node = fetchC_node()
        node = self.findSucc(rFile.meta.contentHash)

        if (node.port == self.node.port and node.ip == self.node.ip):
            if (os.path.isfile(os.getcwd() + "/" + rFile.meta.filename)):
                #if requested file does exists then the file contents should be overwritten, and the version +=
                with open(rFile.meta.filename, 'r+') as file:
                    fileContents = file.readlines()         #returns a list of lines in file
                    file.seek(0)
                    file.truncate()
                    file.write('filename=%s\n' % rFile.meta.filename)
                    file.write('version=%s\n' % str(int(fileContents[1].strip().split('=')[1]) + 1))
                    file.write('owner=%s\n' % rFile.meta.owner)
                    file.write('contentHash=%s\n' % rFile.meta.contentHash)
                    file.write('content=%s\n' % rFile.content)
                    file.close()
            else:#file doesn't exists so create a one with version = 0
                with open(rFile.meta.filename, 'w+') as file:
                    file.write('filename=%s\n' % rFile.meta.filename)
                    file.write('version=0\n')
                    file.write('owner=%s\n' % rFile.meta.owner)
                    file.write('contentHash=%s\n' % rFile.meta.contentHash)
                    file.write('content=%s\n' % rFile.content)
                    file.close()
        else:
            raise customized_exception("Given KEY doesn't match with corresponding NODE")

#RFile readFile(1: string filename, 2: string owner)
#   throws (1: SystemException systemException),

    def readFile(self,filename,owner):
	logger.info("Read of File %s requested by Owner %s: " % (filename, owner))
	self.c_node = fetchC_node()
        node = self.findSucc(hashlib.sha256(owner + ":" + filename).hexdigest())

        if (node.port == self.node.port and node.ip == self.node.ip):
            if (os.path.isfile(os.getcwd() + "/" + filename)):
                #JUst open the requested file
                with open(filename, 'r') as file:
                    fileContents = file.readlines()         #returns a list of lines in file
                    #now checking if given file is owned by it.
                    if (str(owner) == str(fileContents[2].strip().split('=')[1])):
                        rMetadata = RFileMetadata()
                        rMetadata.filename = fileContents[0].strip().split('=')[1]
                        rMetadata.version = int(fileContents[1].strip().split('=')[1])
                        rMetadata.owner = fileContents[2].strip().split('=')[1]
                        rMetadata.contentHash = fileContents[3].strip().split('=')[1]
                        rFile = RFile()
                        rFile.meta = rMetadata
                        rFile.content = fileContents[4].strip().split('=')[1]
                        return rFile
                        #file.write('filename:%s\n' % rFile.meta.filename)
                        #file.write('version:%s\n' % str(int(fileContents[1].strip().split('=')[1]) + 1))
                        #file.write('contentHash:%s\n' % rFile.meta.contentHash)
                        #file.write('owner:%s\n' % rFile.meta.owner)
                        #file.write('content:%s\n' % rFile.content)
                        #file.close()
                    else:
                        raise customized_exception("Given FILE doesn't exists with corresponding OWNER")
            else:#file doesn't exists so create a one with version = 0
                raise customized_exception("Given FILE doesn't exists with corresponding FILENAME")
        else:
            raise customized_exception("Given KEY doesn't match with corresponding NODE/SERVER")



#NodeID findSucc(1: string key)
#   throws (1: SystemException systemException)

#n.find_successor(key){
#   n' = find_predecessor(key);
#    return n'.sucessor
#}

    def findSucc(self,key):
        if self.node_list:
 	    logger.info("Finding Suc_of_given key: %s " % str(key))
            self.c_node = fetchC_node()
            self.c_node = self.findPred(key)

            transport = TSocket.TSocket(self.c_node.ip, self.c_node.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            fileStoreClient = FileStore.Client(protocol)
            transport.open()
            suc_of_given_key = fileStoreClient.getNodeSucc()
	    logger.info("Suc_of_given key is  at %s: %s" % (suc_of_given_key.port, suc_of_given_key.ip))   
	    return suc_of_given_key
        else:
            raise customized_exception("No Finger_table exists for c_node")

#NodeID findPred(1: string key)
#   throws (1: SystemException systemException),

#n.find_predecessor(key){
#    n' = n;
#    while (key not_belongs_to (n',n'.sucessor)){
#        n' = n'.closest_preceding_finger(key);
#    }
#    return n';
#}
    def findPred(self,key):
        if self.node_list:
            self.c_node = fetchC_node()
            if (self.getNodeSucc().id == self.c_node.id):
                return self.c_node

            p_node = self.c_node.id
            s_node = self.getNodeSucc().id
            if (p_node < s_node):
                belongsToBool = (p_node <= key < s_node)
            else:
                belongsToBool = (p_node <= key or key < s_node)

            if not belongsToBool:
                c_node = self.closest_preceding_finger(key)
		logger.info("Next Suc at %s :  %s " % (c_node.ip, c_node.port))
                
		transport = TSocket.TSocket(self.c_node.id,self.c_node.port)
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                fileStoreClient = FileStore.Client(protocol)
                transport.open()
                pred_of_given_key = fileStoreClient.findPred(key)
                return pred_of_given_key

        else:
            raise customized_exception("No Finger_table exists for c_node")

#n.closest_preceding_finger(key){
#    for i in m_downto_1{
#        if finger[i].node belongs_to (n,key){
#            return finger[i].node;
#        }
#    }
#
#}

    def  closest_preceding_finger(self,key):
        for i in range(255,-1,-1):
            c_node_id = self.node_list[i].id
            p_node_id = self.c_node.id
            s_node_id = key
            if (p_node_id < s_node_id):
                belongsToBool = (p_node_id <= c_node_id < s_node_id)
            else:
                belongsToBool = (p_node_id <= c_node_id or c_node_id < s_node_id)

            if (belongsToBool and (self.node_list[i].id != self.c_node.id)):
                return self.node_list[i]

        return self.c_node


#void setFingertable(1: list<NodeID> node_list),

    def setFingertable(self,node_list):
        try:
	    logger.info("ALL F_table Sets")
            self.node_list = node_list
        except:
            raise system_exception("Error in Finger_table setting")

    def ping(self):
        print('ping()')

    def getNodeSucc(self):
        if self.node_list:
            return self.node_list[0]
        else:
            raise customized_exception("No Finger_table exists for c_node")

if __name__ == '__main__':
    handler = FileStoreHandler()
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    #server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    # You could do one of these for a multithreaded server
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    # server = TServer.TThreadPoolServer(
    #     processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')

