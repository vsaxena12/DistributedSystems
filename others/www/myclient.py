
#!/usr/bin/env python3

import socket

host = '127.0.0.1'
port = 65432

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((host,port))
    s.sendall(b"hy this is my client for demo")
    data = s.recv(4096)


print('Data Received man..!!',repr(data))
