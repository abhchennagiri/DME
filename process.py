import SocketServer
import socket
import sys
import Queue as Q
import pickle
from message import Message
import time
from datetime import datetime
import threading

#Logger code
import logging
logger  = logging.getLogger()
logging.basicConfig(level=logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

IP = "127.0.0.1"                # IP of the localhost

D_PORTS = [
    9000, 
    9001,
    9002,                       # Port Numbers for the Datacenters
    9003,
    9004
]

NUM_DC = 5                      # Number of Datacenters.

MSG_DELAY = 5                   # Delay in number of seconds between for message passing

class TCPHandler(SocketServer.BaseRequestHandler):
         
    def __init__(self,request,client_address,server):
        SocketServer.BaseRequestHandler.__init__(self, request, client_address, server)

    def create_socket(self):
        """
        Creates a socket and returns it
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        return s
    
    def show_queue(self):
        """
        logger.debug(s the elements of the priority queue)
        """
        logger.debug( "------------------------------------------Entering Show Queue----------------------------------------------------------")
        logger.info("Request Queue")
        req_list = []
        while not self.server.q.empty():
            head = self.server.q.get()
            req_list.append(head)
            logger.info( "(%s, D%s, num_tickets:%s)",str(head[0]),str(head[1]),str(head[2]))
        
        for req in req_list:
            self.server.q.put(req)

        logger.debug( "-------------------------------------------Exiting Show_queue------------------------------------------------------------ ")

    
    def send_message(self,msg):
        """ 
        Used by all the the send methods to run as a seperate thread and send it over the network
        """
        logger.debug( "-------------------------------------------Entering send_message ------------------------------------------------------")
        data = pickle.dumps(msg)
        addr = msg.to_dc
        logger.debug( "Sleeping for some time now")
        time.sleep(MSG_DELAY)
        logger.debug( "Sending the msg now")
        s = self.create_socket()
        s.connect(msg.to_dc)
        logger.debug( "Current time is (after the delay):"+ str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        s.send(data)
        s.close()
        logger.debug( "---------------------------------------------Exiting send_message -----------------------------------------------------")
        return True


    def send_request(self,num_tickets_req):
        """
        Send a broadcast request to all the datacenters
        """
        logger.debug( "-----------------------------------------------Entering send_request---------------------------------------------------")
        data_tup = (self.server.clock, self.server.pid, num_tickets_req)
        logger.debug( "Request Message: " + str(data_tup))
        from_dc = (IP, int(D_PORTS[self.server.pid - 1]))
        logger.debug( "from port: "+str(from_dc))
        for port in D_PORTS:
            if port != int(D_PORTS[self.server.pid - 1]):    
                msg = Message(Message.MSG_SER_REQUEST, from_dc, (IP,int(port)), data_tup )
                threading.Thread(target = self.send_message, args = (msg,)).start()
        logger.debug( "--------------------------------------------------Exiting send_request ------------------------------------------------" )        

    def insert_request_into_queue(self,msg,n):
        """
        Inserts (clock,pid) into the queue.

        """
        logger.debug( "-----------------------------------------------Entering insert_request_queue ------------------------------------------")

        if(msg.message_type == Message.MSG_CLI_REQ):
            req_tup = (self.server.clock,self.server.pid, n)
            logger.debug( "Inserting (self.server.clock,self.server.pid,msg.data[2]) ---> "+ str(req_tup))
            self.server.q.put(req_tup)
         
        else:
            req_tup = (msg.data[0],msg.data[1],msg.data[2])
            logger.debug( "Inserting (msg.data[0],msg.data[1],msg.data[2]) ---> "+ str(req_tup))
            self.server.q.put(req_tup)
            
        logger.debug( "-----------------------------------------------Exiting insert_request_queue -------------------------------------------")
        

    def send_reply(self,to_dc):
        """
        Send a reply to the requesting datacenter
        """
        logger.debug( "----------------------------------------------Entering send_reply-----------------------------------------------------")
        data_tup = (self.server.clock,self.server.pid)
        logger.info( "Reply Message: "+ str(data_tup))
        logger.debug( "Replying to Datacenter which sent the request --> "+ str(to_dc))
        from_dc = (IP, int(D_PORTS[self.server.pid - 1]))
        msg = Message(Message.MSG_SER_REPLY, from_dc, to_dc, data_tup )
        threading.Thread(target = self.send_message, args = (msg,)).start()
        logger.debug( "-----------------------------------------------Exiting send_reply -----------------------------------------------------")
        

    def send_release(self,num_tickets_req):
        """
        Helper function to send release message by the requesting datacenter to all other datacenters
        """
        logger.debug( "--------------------------------------------Entering send_release------------------------------------------------------")
        data_tup = (self.server.clock,self.server.pid,num_tickets_req)
        logger.info( "Release Message: " + str(data_tup))
        from_dc = (IP, int(D_PORTS[int(self.server.pid) - 1]))
        logger.debug( "Sending the release msg from: " + str(from_dc))
        for port in D_PORTS:
            if port != int(D_PORTS[self.server.pid - 1]):
                msg = Message(Message.MSG_SER_RELEASE,from_dc, (IP,port), data_tup)
                threading.Thread(target = self.send_message, args = (msg,)).start()

        logger.debug( "--------------------------------------------Exiting send_release ------------------------------------------------------" )       

    def send_response_client(self, result):
        """
        Helper function to send the response back to the requesting client
        """

        logger.debug( " ------------------------------------------------Entering send_response_client-----------------------------------------")
        from_dc = 0
        to_dc = self.server.cli_addr
        logger.debug( "to_dc: "+ str(to_dc))
        if result == True:
            data = "Ticket purchase successful"
            msg = Message(Message.MSG_CLI_RESPONSE, from_dc, to_dc, data)
            threading.Thread(target = self.send_message, args = (msg,)).start()
        else:
            data = "Not enough tickets remaining"
            msg = Message(Message.MSG_CLI_RESPONSE, from_dc, to_dc, data)
            threading.Thread(target = self.send_message, args = (msg,)).start()
        logger.debug( "--------------------------------------------------Exiting send_response_client-----------------------------------------"  )  

    def handle(self):
       """
       Each Request is handled by a seperate instantiation of the RequestHandler Object by the handle() method.
       """
       self.data = True
       while self.data:  
            self.data = self.request.recv(1024)
            msg =  pickle.loads(self.data)
            logger.debug( " ---------------------------------------msg Details:---------------------------------------------------------")
            logger.debug( "msg.data: " +str(msg.data))
            logger.debug( "msg.message_type 0:'MSG_SER_REQUEST',1:'MSG_SER_RECEIVE',2:'MSG_SER_REPLY',3:'MSG_CLI_REQ',4:'MSG_SER_RELEASE' "+ str(msg.message_type))
            logger.debug( "msg.from_datacenter: (IP, Port)"+str(msg.from_dc))
            logger.debug( "msg.to_datacenter(IP,Port)"+ str(msg.to_dc))
            logger.debug( "----------------------------------------msg Details over:-------------------------------------------------------")
            

            if(msg.message_type == Message.MSG_CLI_REQ):
                logger.info( 'MSG from the Client received')
                num_tickets_req = int(msg.data[4:])
                logger.debug( "Num Tickets Requested requested by the client: "+ str(num_tickets_req))
                self.server.clock += 1
                self.server.cli_addr = msg.from_dc
                logger.debug( "Clock incremented by 1")
                logger.info("Client Request : " + str(num_tickets_req))
                logger.info("Lamport Clock: (%s,D%s)",self.server.clock,self.server.pid)
                self.insert_request_into_queue(msg,num_tickets_req)
                self.send_request(num_tickets_req)
                #threading.Thread(target = self.send_request, args = (num_tickets_req)).start() # Send a request to all other datacenters
                logger.debug( "printing Queue in MSG_CLI_REQ")
                #self.show_queue()
                break

            if(msg.message_type == Message.MSG_SER_REQUEST):
                logger.info( 'MSG from a Data Center Received')
                #logger.debug( "Got the server request"
                num_tickets_req = msg.data[2]
                logger.debug( "Num tickets requested : "+ str(num_tickets_req))
                self.server.clock = max(msg.data[0],self.server.clock) + 1
                logger.debug( "Clock value: ")
                logger.info("Server Request from D%s to D%s", msg.data[1],self.server.pid )
                logger.info("Lamport Clock: (%s,D%s)",self.server.clock,self.server.pid)
                self.insert_request_into_queue(msg,0)
                #logger.debug( "Sending the reply to:", msg.from_dc  )
                self.send_reply(msg.from_dc)
                break

            if(msg.message_type == Message.MSG_SER_REPLY):
                # Increment the number of replies
                # Add a check to see if this reply reaches n-1 and if the request is at the top of the queue. Send a release message.
                logger.debug( 'MSG_SER_REPLY i.e. REPLY MSG from the DatacenterReceived')
                self.server.clock = max(msg.data[0],self.server.clock)
                logger.debug( "Clock value: "+str(self.server.clock))
                self.server.numReplies = self.server.numReplies + 1
                logger.info("Reply from D%s to D%s", msg.data[1],self.server.pid)
                #logger.debug( "logger.debug(ing the type of num_replies, NUM_DC: ", type(num_replies), type(NUM_DC) 
                if self.server.numReplies == (NUM_DC - 1):
                    logger.debug( "Coming after NUM_DC -1")
                    self.server.clock = max(msg.data[0],self.server.clock) + 1
                    logger.info("Lamport Clock: (%s,D%s)",self.server.clock,self.server.pid)
                    queue_head = self.server.q.get()
                    logger.debug( "Queue Head: "+ str(queue_head))
                    if (queue_head[1] == self.server.pid):  
                        # Add the check for number of tickets excedding the pool
                        logger.debug( "Coming here")
                        self.server.numReplies = 0 
                        if(queue_head[2] <= self.server.numTickets):
                            self.server.numTickets -= queue_head[2]
                            logger.info("No of Tickets in the pool:" + str(self.server.numTickets))
                            self.send_release(int(queue_head[2]))
                            #result = True
                            self.send_response_client(True)

                        else:    
                                
                            self.send_release(0)
                            logger.info("No of Tickets in the pool:" + str(self.server.numTickets))
                            self.send_response_client(False)
                    else:
                        self.server.q.put(queue_head)
                logger.debug( "Coming before break")
                break

            if(msg.message_type == Message.MSG_SER_RELEASE):
                #Decrement the number of tickets
                #Remove the top of the queue
                #Process the top of the queue
                logger.debug( "MSG_SER_RELEASE i.e. RELEASE MSG from the DatacenterReceived")
                num_tickets_req = msg.data[2]
                self.server.numTickets -= num_tickets_req
                self.server.q.get()
                self.server.clock = max(msg.data[0], self.server.clock) + 1
                logger.info("Release from D%s to D%s", msg.data[1],self.server.pid)
                logger.info("Lamport Clock: (%s,D%s)",self.server.clock,self.server.pid)
                logger.debug( "Request Queue 2: "+ str(self.server.q.queue))
                self.show_queue()
                if self.server.numReplies == (NUM_DC - 1):
		        queue_head = self.server.q.get()
		        if(queue_head[1] == self.server.pid):
		           self.server.numReplies = 0     
		           if(queue_head[2] <= self.server.numTickets):     
		                self.server.numTickets -= queue_head[2]
                                logger.info("No of Tickets in the pool:" + str(self.server.numTickets))
		                self.send_release(int(queue_head[2]))
		                #result = True
		                self.send_response_client(True)
		           else:    
		                self.send_release(0)
                                logger.info("No of Tickets in the pool:" + str(self.server.numTickets))
		                self.send_response_client(False)
		        else:
		           self.server.q.put(queue_head)
            break
    
       self.request.close()     

 
class Datacenter(SocketServer.TCPServer):
    
    def __init__(self, server_address, handler_class=TCPHandler):
        #self.logger = logging.getLogger('EchoServer')
        #self.logger.debug('__init__')
        self.numTickets = 100
        self.q = Q.PriorityQueue()
        self.clock = 0 
        self.numReplies = 0
        self.cli_addr = ("127.0.0.1",0)
        SocketServer.TCPServer.__init__(self, server_address, handler_class)
        return
        

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print 'Usage: python process.py <process_num>, process_num = [1,5]'
        sys.exit(0)
    #Setting Logging Parameters
    log_file = 'log_dc' + sys.argv[1]
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    HOST= "localhost"
    port = D_PORTS[int(sys.argv[1]) - 1]
    while True:
        server = Datacenter((HOST, port), TCPHandler)
        server.pid = int(sys.argv[1])
        server.serve_forever()

        
