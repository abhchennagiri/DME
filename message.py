"""
Message Class:  Datastructure defined to encapsulate the message.

"""

class Message(object):

    MSG_SER_REQUEST = 0 # Request sent by the Datacenter(requested by client) to every other Datacenter.
    MSG_SER_RECEIVE = 1 # Not being used.
    MSG_SER_REPLY = 2   # Reply message sent by the Datacenter to the requesting Datacenter.
    MSG_CLI_REQ = 3     # Request issued by the client to the datacenter to which it is connected.
    MSG_SER_RELEASE = 4 # Release message sent by the Datacenter which has access to the critical section.
    MSG_CLI_RESPONSE = 5# Response sent by the datacenter to the requesting client.
    
    MSG_TYPE = ['MSG_SER_REQUEST','MSG_SER_RECEIVE','MSG_SER_REPLY','MSG_CLI_REQ','MSG_SER_RELEASE','MSG_CLI_RESPONSE']

    def __init__(self, message_type, from_dc, to_dc, data=None):
        self.message_type = message_type
        self.from_dc = from_dc
        self.to_dc = to_dc
        self.data = data



if __name__ == '__main__':
       pass

       
                                  

    
