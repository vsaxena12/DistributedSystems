#!/usr/bin/env python

import sys
import glob
import hashlib as _hl
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
import chord.ttypes as _tt

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


def main():
    transport = TSocket.TSocket(sys.argv[1], int(sys.argv[2]))
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = FileStore.Client(protocol)
    transport.open()
    test_hash = _hl.sha256('rajar.txt').hexdigest()
    print("\n********** "+str(test_hash)+"**********\n")
    test_node = client.findSucc(test_hash)
    print("\n\n-------- 1 ---"+str(test_node)+"------\n")
    transport.close()

    transport = TSocket.TSocket(test_node.ip, test_node.port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = FileStore.Client(protocol)
    transport.open()
    meta = _tt.RFileMetadata()
    meta.filename = 'rajar.txt'
    meta.version  = 0
    w_file = _tt.RFile()
    w_file.meta = meta
    w_file.content = "<h1>This is great, just it should work!<h1>"
    w_file.meta.contentHash = _hl.sha256(w_file.content).hexdigest()
    client.writeFile(w_file)
    transport.close()
    print("\n\n-------- 2 --- write ------\n")

    transport = TSocket.TSocket(sys.argv[1], int(sys.argv[2]))
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = FileStore.Client(protocol)
    transport.open()
    test_hash = _hl.sha256('rajar.txt').hexdigest()
    test_node = client.findSucc(test_hash)
    print(test_node)
    transport.close()

    transport = TSocket.TSocket(test_node.ip, test_node.port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = FileStore.Client(protocol)
    transport.open()
    print(client.readFile('rajar.txt'))
    transport.close()
    print("\n\n-------- 3 --- read correct ------\n")


    transport = TSocket.TSocket(sys.argv[1], int(sys.argv[2]))
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = FileStore.Client(protocol)
    transport.open()
    test_hash = _hl.sha256('temp.pdf').hexdigest()
    print("\n((((((((()))))))))"+str(test_hash)+"((((((()))))))\n")
    test_node = client.findSucc(test_hash)
    print(test_node)
    transport.close()

    transport = TSocket.TSocket(test_node.ip, test_node.port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = FileStore.Client(protocol)
    transport.open()
    print(client.readFile('temp.pdf'))
    transport.close()
    print("\n\n-------- 3 --- read correct ------\n")


if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)
