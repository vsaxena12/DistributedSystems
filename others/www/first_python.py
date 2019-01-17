#!/usr/bin/env python3

import socket

host = '127.0.0.1'
port = 65432

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((host, port))
    s.listen()
    conn, addr = s.accept()
    with conn:
        print('connection established',addr)
        while True:
            data = conn.recv(4096)
            if not data:
                break
            conn.sendall(data)
