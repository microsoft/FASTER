import socketserver
import socket
import sys
import subprocess

args = []
class ScriptServer(socketserver.BaseRequestHandler):
    def handle(self):
        process = subprocess.Popen(["Z:\\FASTER\\cs\\DprMicrobench\\DprMicrobench\\bin\\Release\\netcoreapp3.1\\DprMicrobench.exe"] + args, stdout=subprocess.PIPE)
        print("started benchmark")
        for line in process.stdout:
            print(line)
        print("exited benchmark")

if __name__ == "__main__":
    args = sys.argv
    host = socket.gethostbyname(socket.gethostname())
    server = socketserver.TCPServer((host, 15000), ScriptServer)
    server.serve_forever()


