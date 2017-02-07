import socket
import sys
from message import Message
import pickle
import time
import SocketServer
import threading
import logging

logging.basicConfig(level=logging.INFO)
IP = '127.0.0.1'
D_PORT = {1:"9000", 2:"9001",3:"9002",4:"9003",5:"9004"}
CLIENT_PORT = {1:5000,2:5001,3:5002,4:5003,5:5004}

class Client():
    def __init__(self, dc_num):
        self.dc_port = D_PORT[dc_num]
        #self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self.serv_addr = (IP, int(self.dc_port))
        #self.sock.connect(self.serv_addr)

    """    
    def send_message(self,msg):
        data = pickle.dumps(msg)
        addr = msg.to_dc
        print "Preparing to send now", data
        time.sleep(10)
        print "Sending the msg now"
        self.sock.sendto(data,addr)
        return True
    
    """
    def send_message(self,msg):
        logging.debug("--------------------------------------------Entering send_message -------------------------------------------")
        data = pickle.dumps(msg)
        addr = msg.to_dc
        #print "Preparing to send now", data
        logging.debug("Sleeping for some time now")
        time.sleep(10)
        logging.debug("Sending the msg now")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self.sock.sendto(data,addr)
        s.connect(msg.to_dc)
        logmsg="Msg data: "+msg.data
        logging.info(logmsg)
        s.send(data)
        s.close()
        logging.debug("-------------------------------------------Exiting send_message ---------------------------------------------------")
        return True


    def send_request(self,data,dc_num):
        #message = "I did it"
        #num_tickets = int(data[4:]) 
        """
        Send a request which has the number of tickets
        """
        from_cli = (IP,CLIENT_PORT[dc_num])
        msg = Message(Message.MSG_CLI_REQ, from_cli, (IP,int(self.dc_port)),data )
        threading.Thread(target = self.send_message, args = (msg,)).start()

    def recv_message(self):
        data = self.sock.recv(1024)
        #print data
        logging.info(data)

class ClientRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        # Echo the back to the client
        data = self.request.recv(1024)
        msg = pickle.loads(data)
        #print "Response: ", msg.data
        logmsg="Response: "+msg.data
        logging.info(logmsg)
        return  


def createServer(dc_num):
        
       address = (IP,CLIENT_PORT[dc_num]) 
       server =  SocketServer.TCPServer(address, ClientRequestHandler)
       server.serve_forever() 

if __name__ == "__main__":
       dc_num = int(raw_input("Enter the datacenter number that you want to connect to\n"))
       server_thread = threading.Thread(target = createServer,args = (dc_num,)) 
       server_thread.start() 
       client1 = Client(dc_num)
       proceed=1
       while True:
        line = raw_input("")
        word=(line[0:3])
        if(word!="buy"):
            #logging.info("Input should be of the form 'Buy x' where x is number of tickets")
            print "Input should be of the form 'Buy x' where x is number of tickets"
            proceed=0
        if(proceed==1):
            tickint=(int)(line[4:])
            if(tickint<=0):
                #print "Try to purchase minimum of 1 ticket"
                #logging.info("Minimum purchase request considered -> 1 ticket") 
                print "Minimum purchase request considered -> 1 ticket"
                proceed=0
        if(proceed==1): 
            client1.send_request(line,dc_num)    # * if user just pressed Enter line will
        proceed=1
