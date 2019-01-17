#!/usr/bin/python
import socket
import sys
import signal
import time
import threading
import os
import mimetypes
import datetime

Dict = {}

class MyWebServer:
    """
    Class for better understaning
    """


    def __init__(self,port=8080):
        self.host = ''
        self.port = 0
        self.cur_cont_dir = "www"

    def activate_server(self):
        self.socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)


        try:
            self.socket.bind((self.host, self.port))
            connname = socket.gethostname()
            conne = socket.gethostbyname(connname)
            portnum = self.socket.getsockname()[1]
            print (" ------------SERVER STARTED ON ",connname,":",conne,portnum,"-----------")


        except Exception as exception:
            print("-------------------------- ERROR IN BINDING -------------------------")
            self.shutdown()
            print("-------------------------- SHUTTING DOWN THE SERVER-------------------------")
            sys.exit(1)

        print("------------------------ To SHUTDOWN THE SERVER: cntrl+c ----------------------- ")
        self._waiting_for_connections()


    def shutdown(self):
        try:
            print("-------------------------- SHUTTING DOWN THE SERVER-------------------------")
            server.socket.shutdown(socket.SHUT_RDWR)

        except Exception as exception:
            print("---------------------- ERROR IN SHUTTING DOWN THE SERVER ---------------------",exception)

    def _generate_headers(self, response_code,f_size,f_type,l_time):
        header = ''
        if response_code == 200:
            header += 'HTTP/1.1 200 OK\n'
        elif response_code == 404:
            header += 'HTTP/1.1 404 Not Found\n'

        cur_date = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
#        print(l_time)
    #    print(f_size)
        header += 'Date: ' + cur_date +'\n'
        header += 'Server: Pthon Server by Savan Desai\n'
        header += 'Last Modified: ' + l_time +'\n'
        header += 'Content-Type: ' +f_type+'\n'
        header += 'Content-Length: ' +f_size+'\n'
        header += 'Connection: close\n\n' # Signal that connection will be closed after completing the request

        return header


    def _waiting_for_connections(self):
        self.socket.listen(5)

        while True:
            conn, addr = self.socket.accept()
            connname = socket.gethostname()
            conne = socket.gethostbyname(connname)
            #client.settimeout(60)
            print("------------------- Recieved connection from: ",addr,"---------------------")
            threading.Thread(target=self._handle_client, args=(conn, addr)).start()

    def _handle_client(self, client, address):
        PACKET_SIZE = 1024
        while True:
            print("CLIENT",client)

            data = client.recv(1024) # Recieve data packet from client and decode
            string = data.decode("utf-8")

            request_method = string.split(' ')[0]
            print ("Method:",request_method)
            print("Request Body:",string)

            if request_method == "GET":
                file_requested = string.split(' ')              #look for a whitespace after GET
                file_requested =  file_requested[1]             #2nd eleement
                #file_requested = file_requested.split('?')[0]   #discard everything after ?

                if file_requested == "/":                       #just load by default file
                    file_requested = "/index.html"

                if file_requested in Dict:
                    Dict[file_requested] += 1
                    no_acc = Dict[file_requested]
                else:
                    Dict[file_requested]=1
                    no_acc=1

                d_filename = file_requested
                file_requested = self.cur_cont_dir + file_requested
                print ("Serving web page [",file_requested,"]")

                try:
                    file_handler = open(file_requested, 'rb')
                    response_content = file_handler.read()
                    fsize = os.path.getsize(file_requested)
                    ftype = mimetypes.guess_type(file_requested, strict=True)
                    fmtime = os.path.getmtime(file_requested)
                    flastmodi=datetime.datetime.fromtimestamp(fmtime)
                    if ftype == None:
                        ftype = "application/octet-stream"

                    #print("\n",ftype,"\n")
                    file_handler.close()
                    response_headers = self._generate_headers(200,str(fsize),str(ftype),str(flastmodi))


                except Exception as e:
                    print("FILE NOT FOUND.. SERVING RESPONSE CODE 404")
                    fsize=0
                    ftype ="application/octet-stream"
                    flastmodi = ''
                    response_headers = self._generate_headers(404,str(fsize),str(ftype),str(flastmodi))
                    response_content = b"<html><body><p>ERROR 404 FILENOT FOUND IN WWW </p><p>MY SIMPLE PYTHON HTTP SERVER</p></body></html>"

                server_response =  response_headers.encode("utf-8")
                server_response +=  response_content

                client.send(server_response)
                client.close()
                print("{}|{}|{}|{}".format(d_filename,address[0],address[1],no_acc))
                break
            else:
                print("Unknown HTTP request method:",request_method)

def graceful_shutdown(sig, unused):
    server.shutdown() #shut down the server
    import sys
    sys.exit(1)


signal.signal(signal.SIGINT, graceful_shutdown)
server = MyWebServer(8080)
server.activate_server()
