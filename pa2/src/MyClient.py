import sys
import glob

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/src_code/lib/python2.7/site-packages')[0]) # path where thrift is installed

from chord import FileStore
from chord.ttypes import NodeID, RFile, RFileMetadata, SystemException

from hashlib import sha256

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


def make_socket(ipAddress, port):
    transport = TSocket.TSocket(ipAddress, port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    file_store = FileStore.Client(protocol)
    transport.open()
    return file_store;


class TestChord:
    transport = TSocket.TSocket(sys.argv[1], int(sys.argv[2]))
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    filestore = FileStore.Client(protocol)
    transport.open()

    def __init__(self):
        pass

    def test_write_file(self, rfile):
        node = self.filestore.findSucc(rfile.meta.contentHash)
        print "node id: ", node
        assert node.id == "162f2ef78020a93545457290a21d4ea634d4bca22aff8530e2011209be88ff82", "Test_Write_file - Node returned is wrong"
        print "Test case passed 1 -- get successor node for key: ", rfile.meta.contentHash


        file_store = make_socket(node.ip, node.port)
        file_store.writeFile(rfile)
        read_file = file_store.readFile(rfile.meta.filename, rfile.meta.owner)
        assert str(read_file.meta.filename) == str(rfile.meta.filename), "Test_Write_file - Filename mismatch"
        assert str(read_file.meta.version) == str(rfile.meta.version), "Test_Write_file - version mismatch"
        assert str(read_file.meta.owner) == str(rfile.meta.owner), "Test_Write_file - owner mismatch"
        assert str(read_file.meta.contentHash) == str(rfile.meta.contentHash), "Test_Write_file - contentHash mismatch"
        assert str(read_file.content) == str(rfile.content), "Test_Write_file - Content mismatch"
        print "Test case passed 2 -- Write file: file name - %s, owner - %s" % (rfile.meta.filename, rfile.meta.owner)


if __name__ == '__main__':
    rf_metadata = RFileMetadata()
    rf_metadata.filename = "file_name"
    rf_metadata.version = 0
    rf_metadata.owner = "owner_name"
    rf_metadata.contentHash = sha256(rf_metadata.owner + ":" + rf_metadata.filename).hexdigest()
    rfile = RFile()
    rfile.meta = rf_metadata
    rfile.content = "This is my first apache thrift programming experience"

    try:
        t = TestChord()
        t.test_write_file(rfile)
    #    t.test_read_file(rf_metadata.filename, rf_metadata.owner)
        #t.test_negative_cases(make_socket('128.226.180.166', 9095), rfile.meta.contentHash)
    except Thrift.TException as tx:
        print('%s' % tx.message)
