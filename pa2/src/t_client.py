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

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

# Other importent functionalities as per requirements

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MY_CHORD_CLIENT")

def main():
    transport = TSocket.TSocket('128.226.114.203',9091)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = FileStore.Client(protocol)
    transport.open()

    meta = RFileMetadata()
    meta.filename = 'savan.txt'
    meta.version  = int(0)
    meta.owner = 'imowner'
    meta.contentHash = hashlib.sha256(meta.owner + ":" + meta.filename).hexdigest()
    r_file = RFile()
    r_file.meta = meta
    r_file.content = "testcases are cleared"
    client.writeFile(r_file)
    f_name = "savan.txt"
    f_owner = "imowner"
    read_file = client.readFile(f_name,f_owner)
    transport.close()

if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)

