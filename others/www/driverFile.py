
def closeDownTheServer(sig,unused):
    server.shutdown()
    sys.exit(1)

signal.signal(signal.SIGINT, closeDownTheServer)
server = MyWebServer(8080)
server.start()

print("To CLOSEDOWN THE SERVER: cntrl+c ")
