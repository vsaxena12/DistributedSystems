import glob
import logging
import os
import socket
import sys

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/src_code/lib/python2.7/site-packages')[0]) # path where thrift is installed

from chord import FileStore
from chord.ttypes import NodeID, RFile, RFileMetadata, SystemException

from hashlib import sha256

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CHORD")


def belongs_to(current_node, preceding_node, successor_node):
    if preceding_node < successor_node:
        return preceding_node <= current_node < successor_node
    return preceding_node <= current_node or current_node < successor_node


def system_exception(msg):
    sx = SystemException()
    sx.message = msg
    return sx


def node_instance(ip, port, id):
    n = NodeID()
    n.ip = ip
    n.port = int(port)
    n.id = id
    return n;


def make_socket(ip_address, port):
    transport = TSocket.TSocket(ip_address, port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    file_store = FileStore.Client(protocol)
    transport.open()
    return file_store;


def getCurrentNode():
    ip = socket.gethostbyname(socket.getfqdn())
    port = sys.argv[1]
    id = sha256(str(ip) + ":" + str(port)).hexdigest()
    return node_instance(ip, port, id)


class FileStoreHandler:
    node = getCurrentNode()

    def __init__(self):
        self.node_list = []
        self.curr_node = self.node

    def writeFile(self, r_file):
        logger.info("Request to Write File %s by owner %s: " % (r_file.meta.filename, r_file.meta.owner))
        self.curr_node = getCurrentNode()
        node = self.findSucc(r_file.meta.contentHash)

        if node.port == self.node.port and node.ip == self.node.ip:
            if os.path.isfile(os.getcwd() + "/" + r_file.meta.filename):
                with open(r_file.meta.filename, 'r+') as f:
                    data = f.readlines()
                    f.seek(0)
                    f.truncate()
                    f.write('filename=%s\n' % r_file.meta.filename)
                    f.write('version=%s\n' % str(int(data[1].strip().split('=')[1]) + 1))
                    f.write('owner=%s\n' % r_file.meta.owner)
                    f.write('contentHash=%s\n' % r_file.meta.contentHash)
                    f.write('content=%s\n' % r_file.content)
                    f.close()
            else:
                with open(r_file.meta.filename, 'w+') as f:
                    f.write('filename=%s\n' % r_file.meta.filename)
                    f.write('version=0\n')
                    f.write('owner=%s\n' % r_file.meta.owner)
                    f.write('contentHash=%s\n' % r_file.meta.contentHash)
                    f.write('content=%s\n' % r_file.content)
                    f.close()
        else:
            raise system_exception("Key is not associated with the node")

    def readFile(self, filename, owner):
        logger.info("Request to Read file %s by owner %s" % (filename, owner))
        self.curr_node = getCurrentNode()
        node = self.findSucc(sha256(owner + ":" + filename).hexdigest())

        if node.port == self.node.port and node.ip == self.node.ip:
            if os.path.isfile(os.getcwd() + "/" + filename):
                with open(filename, 'r') as f:
                    data = f.readlines()
                    if str(owner) == str(data[2].strip().split('=')[1]):
                        rf_metadata = RFileMetadata()
                        rf_metadata.filename = data[0].strip().split('=')[1]
                        rf_metadata.version = int(data[1].strip().split('=')[1])
                        rf_metadata.owner = data[2].strip().split('=')[1]
                        rf_metadata.contentHash = data[3].strip().split('=')[1]
                        r_file = RFile()
                        r_file.meta = rf_metadata
                        r_file.content = data[4].strip().split('=')[1]
                        return r_file
                    else:
                        logger.error("File %s does not exist for the given owner %s" % (filename, owner))
                        raise system_exception("File does not exist with the given owner name")
            else:
                logger.error("No file exist with the name %s" % filename)
                raise system_exception("No file exist with the name%s" % filename)
        else:
            logger.error("Key is not associated with the node")
            raise system_exception("Key is not associated with the node")

    def setFingertable(self, node_list):
        try:
            self.node_list = node_list
            logger.info("Finger table has been set")
        except:
            raise system_exception("Cannot set finger table or cannot get the nodes list")

    def findSucc(self, key):
        if not self.node_list:
            raise system_exception("Fingertable not exist for the current node")

        logger.info("Routing to find the successor node with key : %s " % str(key))
        self.curr_node = getCurrentNode()
        self.curr_node = self.findPred(key)
        targetNode = make_socket(self.curr_node.ip, self.curr_node.port).getNodeSucc()
        logger.info("Successor is at port %s on host %s" % (targetNode.port, targetNode.ip))
        return targetNode

    def findPred(self, key):
        if not self.node_list:
            raise system_exception("Fingertable not exist for the current node")

        self.curr_node = getCurrentNode()

        if self.getNodeSucc().id == self.curr_node.id:
            return self.curr_node
        """
        if belongs_to(key, self.curr_node.id, self.getNodeSucc().id):
            return self.curr_node
        """

        if not belongs_to(key, self.curr_node.id, self.getNodeSucc().id):
            curr_node = self.closest_preceding_finger(key)
            logger.info("Next successor is at ip %s on port %s " % (curr_node.ip, curr_node.port))
            return make_socket(curr_node.ip, curr_node.port).findPred(key)

    def closest_preceding_finger(self, key):
        for x in range(255, -1, -1):
            if belongs_to(self.node_list[x].id, self.curr_node.id, key) and self.node_list[x].id != self.curr_node.id:
                return self.node_list[x]
        return self.curr_node

    def getNodeSucc(self):
        if not self.node_list:
            raise system_exception("Fingertable not exist for the current node")

        return self.node_list[0]


if __name__ == '__main__':
    try:
        handler = FileStoreHandler()
        processor = FileStore.Processor(handler)
        transport = TSocket.TServerSocket(port=sys.argv[1])
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

        logger.info('Starting the server...')
        logger.info('...')
        server.serve()
        logger.info('done.')
    except KeyboardInterrupt:
logger.info('Server stopped...')
