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
import sys

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from chord.ttypes import NodeID, RFile, RFileMetadata, SystemException


from tutorial.ttypes import InvalidOperation, Operation

from shared.ttypes import SharedStruct

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

# Other importent functionalities as per requirements

def fetchC_node():
    node = NodeID()
    node.ip = socket.gethostbyname(socket.gethostname())
    node.port = int(sys.argv[1])
    node.id = hashlib.sha256(str(node.ip) + ":" + str(node.port)).hexdigest()
    return node;

def customized_exception(error_message):
    myException = SystemException()
    myException.message = error_message
return myException

class FileStoreHandler:
    node = NodeID()
    node.ip = socket.gethostbyname(socket.gethostname())
    node.port = int(sys.argv[1])
    node.id = hashlib.sha256(str(node.ip) + ":" + str(node.port)).hexdigest()

    def __init__(self):
        self.log = {}
        self.node_list = []
        self.c_node = self.node

    def writeFile(self,rFile):
        self.c_node = fetchC_node()
        node = self.c_node

        if (node.port == self.node.port and node.ip == self.node.ip):
            if (os.path.isfile(os.getcwd() + "/" + rFile.meta.filename):):
                #if requested file does exists then the file contents should be overwritten, and the version +=
                with open(rFile.meta.filename, 'r+') as file:
                    fileContents = file.readlines()         #returns a list of lines in file
                    file.seek(0)
                    file.truncate()
                    file.write('filename=%s\n' % rFile.meta.filename)
                    file.write('version=%s\n' % str(int(fileContents[1].strip().split('=')[1]) + 1))
                    file.write('contentHash=%s\n' % rFile.meta.contentHash)
                    file.write('owner=%s\n' % rFile.meta.owner)
                    file.write('content=%s\n' % rFile.content)
                    file.close()
            else:#file doesn't exists so create a one with version = 0
                with open(rFile.meta.filename, 'w+') as file:
                    file.write('filename=%s\n' % rFile.meta.filename)
                    file.write('version=0\n')
                    file.write('contentHash=%s\n' % rFile.meta.contentHash)
                    file.write('owner=%s\n' % rFile.meta.owner)
                    file.write('content=%s\n' % rFile.content)
                    file.close()
        else:
            # we need to raise exception for every specifiv error
            raise customized_exception("Given KEY doesn't match with corresponding NODE")

    def ping(self):
        print('ping()')


    def zip(self):
        print('zip()')

if __name__ == '__main__':
    handler = FileStoreHandler()
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    # You could do one of these for a multithreaded server
    # server = TServer.TThreadedServer(
    #     processor, transport, tfactory, pfactory)
    # server = TServer.TThreadPoolServer(
    #     processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')
