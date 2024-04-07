#!/bin/python3

### TODO: 
# Author name: 


from simulator import *
import time as tm

# Multicast events for the driver
multicast_events = [
    #(time, message_id, sending host_id, message/payload)
    (10, 'M1', 1, 'Januray'),
    (20, 'M2', 1, 'February'),
    (30, 'M3', 1, 'March'),
    (10, 'M4', 2, 'One'),
    (20, 'M5', 2, 'Two'),
    (30, 'M6', 2, 'Three')
]


class Host(Node):
    def __init__(self, sim, host_id):
        Node.__init__(self, sim, host_id)
        self.host_id = host_id
        self.gmembers = []
    
    def initialize(self):
        self.send_sequence = 0 # Initialize a int to hold the sender's sequence number
        self.to_queue = {} # Initialize a dict to hold the TO holdback "queue"
        self.to_request_queue = {} # Initialize a dict to hold the sender's request "queue"
        self.to_ack_queue = {} # Initialize a dict to hold the ack queue.  This code is halfway to fully reliable mcast.
        self.send_id = 0 # Initialize an int to hold the current message send ID
        self.agree_id = 0 # Initialize an int to hold the current/last agreed ID
        self.proposed_id = 0 # Initialize an int to hold the current message proposed ID
        pass

    def multicast(self, time, message_id, message_type, payload): # Since this method is invoked directly by the simulator, some of the message-specific init stuff has to go in here.  Which is why there are two additional ACK methods below.
        self.send_id = self.send_id + 1 # For each message, increment the send sequence by 1
        self.to_request_queue.update({f'{message_id}':{'proposed':[],'agree':-1,'sequence':self.send_id}}) # Add the message to the request "queue"
        print(f'Time {time}:: {self} SENDING mulitcast message [{message_id}] -- [{self.send_id}]')
        for to in self.gmembers: # Multicast
            mcast = Message(message_id, self, to, message_type, self.send_id, self.proposed_id, payload)
            self.send_message(to, mcast)

    def retrycast(self, time, message_id, message_type, payload): # This method could be combined with the below, but since we need separate methods anyways due to the simulator, we might as well save on some space and put some bits in here, because this is called like half a dozen times below.
        print(f'Time {time}:: {self} SENDING mulitcast message [{message_id}] -- [{self.send_id}]')
        self.to_request_queue[f'{message_id}']['proposed'] = [] # WIPE OUT the current proposals in the request queue for the message ID
        sequence = self.to_request_queue[f'{message_id}']['sequence'] # Grab the sequence number, for FIFO
        for to in self.gmembers: # Multicast
            mcast = Message(message_id, self, to, message_type, sequence, self.proposed_id, payload)
            self.send_message(to, mcast)

    def ackcast(self, time, message_id, message_type, payload): # This is a basic multicast method that has no extra functionality, it's mostly used by the ACKs
        print(f'Time {time}:: {self} SENDING mulitcast message [{message_id}] -- [{self.send_id}]')
        for to in self.gmembers: # Multicast
            mcast = Message(message_id, self, to, message_type, self.send_id, self.proposed_id, payload)
            self.send_message(to, mcast)

    def ackdeliver(self): # Method to confirm that all messages have received all the acks and finally deliver
        tempresults = [] # Set an array to store message IDs
        for id in self.to_queue: # For everything in the holdback queue
            if self.to_queue[f'{id}']['agree'] == (self.agree_id + 1): # If any messages have an agreed ID == the current agreed ID + 1 (because, if multiple messages are multicast at the exact same time, this can happen)
                tempresults.append(id) # Add them to the ID array
        if all((len(self.to_ack_queue[f'{id}']['ack']) == len(self.gmembers)) for id in tempresults): # If ALL messages with an agreed ID of the current, self agreed + 1 have received ALL of the ACKs from all processes
            for id in sorted(tempresults): # For each ID
                print(f'{self} is delivering message {id} with an agreement of {self.agree_id}')
                if id in self.to_request_queue: # If the ID is in the request queue (as in, this was the process that sent the original multicast)
                    self.nextorder(id) # Check to see if any other message orders can be sent with the nextorder method
                self.deliver_message(self.to_queue[f'{id}']['time'], self.to_queue[f'{id}']['message']) # Deliver the message
                self.to_queue.pop(f'{id}') # Remove them from the holdback queue
                self.agree_id = self.agree_id + 1 # Increase the last agreed ID
                
    def requestorder(self, message_id, time): # Method for senders to determine the request order
        proposed = self.to_request_queue[f'{message_id}']['proposed'] # Define an array to hold all the proposed values in the request queue
        badagree = True # Set a retry boolean to True
        previous_id = None # Define a value to store the ID of the last sent message
        if len(proposed) == len(self.gmembers): # If the number of proposed values == the number of processes
            if self.to_request_queue[f'{message_id}']['sequence'] == self.send_sequence + 1: # If the next message sequence number is == the sequence number of the last message sent + 1
                for id in self.to_request_queue: # Check all the messages in the request queue
                    if self.to_request_queue[f'{id}']['sequence'] == self.send_sequence: # Find the message ID of the last message sent per the sequence number
                        previous_id = id # Store the message ID into the previous_id variable
                if previous_id == None: # If there IS no previous ID, as in this is the first message sent
                    badagree = False # Set the retry flag to false
                elif len(self.to_ack_queue[f'{previous_id}']['ack']) == len(self.gmembers): # Or, if the previous ID has received all of its ACKs (and thus, has been delivered)
                    badagree = False # Set the retry flag to false
                if not badagree: # If the retry flag is False
                    proposed.sort() # Sort the values in the propose array
                    self.proposed_id = proposed[len(self.gmembers) - 1] # Take the last value in the array, the largest (yes this is a lazy way of doing this), and set it to be the sender's proposed ID
                    if self.proposed_id > self.agree_id: # If the proposed ID is greater than the last agreed ID
                        for id in self.to_queue: # For everything in the holdback queue
                            if self.to_queue[f'{id}']['agree'] == self.proposed_id: # If any message has an agreement that matches the current proposed ID
                                badagree = True # Then this is a bad proposed ID, it's already been proposed and agreed and so should be retried
                                break
                        for id in self.to_request_queue: # If the above didn't break, then for everything in the request queue:
                            if self.to_request_queue[f'{id}']['agree'] == self.proposed_id: # If the current proposed ID matches the last agreed ID of any message
                                badagree = True # Then this is a bad proposed ID, it's already been proposed and agreed and so should be retried
                                break
                        if badagree: # If, at this point, the retry flag has been set:
                            print(f'{self} is restarting the order collection of {message_id}')
                            self.retrycast(time, message_id, 'RETRY_TO', payload=None) # Send out to all of the processes that new proposals are needed
                        else: # If the retry flag HASN'T been set:
                            print(f'{self} is multicasting the order of {message_id} with agreement of {self.proposed_id}')
                            self.retrycast(time, message_id, 'ORDER_TO', payload=None) # Send out the agreed order to all of the processes
                            self.send_sequence = self.send_sequence + 1 # Increase the sent sequence by 1 (to maintain FIFO)
                            self.to_request_queue[f'{message_id}']['agree'] = self.proposed_id # Update the agreed ID in the request queue with the new agreed ID
                            for id in self.to_request_queue: # Now, check the request queue again
                                proposed = self.to_request_queue[f'{id}']['proposed'] # Set the temporary proposed value to the message's proposed value in the request queue
                                if (self.to_request_queue[f'{id}']['sequence'] ==  self.send_sequence + 1) and (len(proposed) == len(self.gmembers)): # If anything is found that has a send sequence that is one greater than what was just sent AND has all of its proposals
                                    proposed.sort() # Sort the proposals
                                    self.proposed_id = proposed[len(self.gmembers) - 1] # Select the largest
                                    to_request = Message(id, self, self, 'REPLAY_TO', self.to_request_queue[f'{id}']['sequence'], self.proposed_id, payload=None) # Define a new REPLAY_TO message for the sender to send to itself to initiate a new requestorder
                                    self.send_message(self, to_request) # Send the REPLAY_TO message to itself
                    elif self.proposed_id <= self.agree_id: # Else, if the agreed proposed ID is LESS than the current agreed ID:
                        print(f'{self} is restarting the order collection of {message_id}')
                        self.retrycast(time, message_id, 'RETRY_TO', payload=None) # This is a bad proposed ID and it needs to be redone
        pass

    def nextorder(self, message_id): # Method very similar to the above, except instead of checking the previous ID, this is invoked when a message gets all of its ACKs (and is delivered) and the following message needs an order
        badagree = False # Set a retry boolean to False, since this time we assume we're good since this is only invoked if the previous message was fully ACK'd and delivered
        next_id = None # Define a variable to store the next message's ID, if any
        current_sequence = self.to_request_queue[f'{message_id}']['sequence'] # Get the current sequence number, based on the message that was just ACK'd
        for id in self.to_request_queue: # Check all the messages in the request queue
            if self.to_request_queue[f'{id}']['sequence'] == current_sequence + 1: # If any of them have a sequence number that is next in line
                    next_id = id # Set next_id to that message ID
        if next_id != None: # So long as the next message exists and is found
            time = self.to_queue[f'{next_id}']['time'] # Since this method is invoked by the delivery of the previous method (and not via a message receive), pull the message's time from the holdback queue (because the request queue is just the message ID and proposed values).  This COULD break if the sender hasn't received it's own message yet
            proposed = self.to_request_queue[f'{next_id}']['proposed'] # Pull the array of proposed values from the request queue for that ID
            if len(proposed) == len(self.gmembers): # If all of the processes have proposed a value
#                if self.to_request_queue[f'{next_id}']['sequence'] == self.send_sequence + 1: # And if the 
#                    for id in self.to_request_queue:
#                        if self.to_request_queue[f'{id}']['sequence'] == self.send_sequence:
#                            previous_id = id
#                    if previous_id == None:
#                        badagree = False
#                    elif len(self.to_ack_queue[f'{previous_id}']['ack']) == len(self.gmembers):
#                        badagree = False
#                if not badagree:
                proposed.sort() # Sort the array of proposals
                self.proposed_id = proposed[len(self.gmembers) - 1] # Set the current proposed ID to the last, largest value (again, this is lazy way of doing this)
                if self.proposed_id > self.agree_id: # If the proposed ID is greater than the last agreed ID
                    for id in self.to_queue: # For all IDs in the holdback queue
                        if self.to_queue[f'{id}']['agree'] == self.proposed_id: # If their agreed IDs match the current proposed ID:
                            badagree = True # Then this is a bad proposal and needs to be retried
                            break
                    for id in self.to_request_queue: # For all ID in the request queue:
                        if self.to_request_queue[f'{id}']['agree'] == self.proposed_id: #If their agreed IDs match the current proposed ID
                            badagree = True # Then this is a bad proposal and needs to be retried
                            break
                    if badagree: # If the proposal is bad and needs retried:
                        print(f'{self} is restarting the order collection of {next_id}')
                        self.retrycast(time, next_id, 'RETRY_TO', payload=None) # Send out an order retry request
                    else: # If the proposal isn't bad
                        print(f'{self} is multicasting the order of {next_id} with agreement of {self.proposed_id}')
                        self.retrycast(time, next_id, 'ORDER_TO', payload=None) # Send out the order for the message ID
                        self.send_sequence = self.send_sequence + 1 # Increase the send sequence by 1, to maintain FIFO
                        self.to_request_queue[f'{next_id}']['agree'] = self.proposed_id # Add the agreed ID to the request queue
                        if self.to_request_queue: # If there's anything in the queue, check for anything with sequence + 1
                            for id in self.to_request_queue: # For everything in the queue
                                proposed = self.to_request_queue[f'{id}']['proposed'] # Set a temporary proposed array
                                if (self.to_request_queue[f'{id}']['sequence'] ==  self.send_sequence + 1) and (len(proposed) == len(self.gmembers)): # If anything in the queue has the next send sequence AND all of the proposals (yes this is a copy of the above, most of this method is)
                                    proposed.sort() # Sort the proposals
                                    self.proposed_id = proposed[len(self.gmembers) - 1] # Take the last value (the largest, I feel like a broken clock)
                                    to_request = Message(id, self, self, 'REPLAY_TO', self.to_request_queue[f'{id}']['sequence'], self.proposed_id, payload=None) # Create new REPLAY_TO message for the sender to send to itself
                                    self.send_message(self, to_request)  # Send the REPLAY_TO message to itself
                elif self.proposed_id <= self.agree_id: # If the proposed ID is less than the last agreed ID: 
                    print(f'{self} is restarting the order collection of {next_id}')
                    self.retrycast(time, next_id, 'RETRY_TO', payload=None) # This is a bad proposal and needs to be retried
            pass

    def totalorder(self, frm, message, time): # On a new message from receive_message:
        type = message.mtype # Get the message type
        print(f'{self} has latest agreement ID of [{self.agree_id}]')
        if type == 'DRIVER_MCAST': # If the message type has a type of DRIVER_MCAST (is a NEW message):
            self.proposed_id = self.agree_id + 1 # Set the proposed id to be the last agreed id + 1
            self.to_queue.update({f'{message.message_id}':{'from': f'{frm}', 'message': message, 'time': time, 'proposed': self.proposed_id, 'agree': self.agree_id}}) # Add the new message to the to_order (holdback) "queue"
            if message.message_id not in self.to_ack_queue: # If it's a new message and not in the separate ACK "queue" (it hasn't gotten any ACKs from anyone else, this is separate from the above because an ACK could be received before the message itself):
                self.to_ack_queue.update({f'{message.message_id}':{'ack':[]}})  # Create a new record in the ACK queue to track the message
            print(f'{self} is sending totalorder request to {frm} for {message.message_id} with proposed ID of {self.proposed_id}')
            to_request = Message(message.message_id, self, frm, 'REQUEST_TO', message.send_id, self.proposed_id, payload=None) # Create a new request to send back to te sender of type REQUEST_TO
            self.send_message(frm, to_request) # Send the message back, asking for the order
        elif (type == 'ORDER_TO'): # If the message has a type of ORDER_TO, which tells the processes the order to deliver a mesasge
            print(f'{self} received order of {self.agree_id} from {frm}')
            self.to_queue[f'{message.message_id}']['agree'] = message.proposed_id # Update the agreed value in the holdback queue
            self.ackcast(time, message.message_id, 'ACK_TO', payload=message.proposed_id) # Multicast an ACK_TO to the group
        elif (type == 'ACK_TO'): # If the message has a type of ACK_TO, which is a acknowledgement that an order has been received
            if message.message_id in self.to_ack_queue: # If the message exists in the ack queue
                ack = self.to_ack_queue[f'{message.message_id}']['ack'] # Define the list of processes that ACKs have been received from
                ack.append(frm) # Add the new host to the ack array
                self.to_ack_queue[f'{message.message_id}']['ack'] = ack # Add the ack array back to the ack queue
                print(f'{self} currently has ACKs for {message.message_id} from: {ack}')
                if len(ack) == len(self.gmembers): # If the number of ACKs == the number of processes in the group
                    self.ackdeliver() # Invoke the ackdeliver message to check if the message can be delivered
            else:
                self.to_ack_queue.update({f'{message.message_id}':{'ack':[frm]}}) # If the message ISN'T in the ACK queue (this is the first ack and the process hasn't received the original message), create a new record in the queue to track that message
        elif (type == 'RETRY_TO'): # If the message has the type of RETRY_TO, which is the original sender asking for new proposals:
            self.proposed_id = self.agree_id + 1 # Set a new proposal
            print(f'{self} received order retry for {message.message_id} from {frm}, sending new proposal of {self.proposed_id}')
            self.to_queue[f'{message.message_id}']['proposed'] = self.proposed_id # Update the proposed value in the holdback queue
            to_request = Message(message.message_id, self, frm, 'REQUEST_TO', message.send_id, self.proposed_id, payload=None) # Create a new message
            self.send_message(frm, to_request) # Send the new proposal back to the sender
        elif (type == 'REQUEST_TO'): # If the message has a type of REQUEST_TO, which is a process asking for an order
            print(f'{self} received totalorder request from {frm} for {message.message_id} with proposed value of {message.proposed_id}')
            proposed = self.to_request_queue[f'{message.message_id}']['proposed'] # Define an array from the existing proposed values (if any)
            proposed.append(message.proposed_id) # Add the proposed value to the array
            self.to_request_queue[f'{message.message_id}']['proposed'] = proposed # Set the proposed values in the request queue to the new array
            print(f'{self} has proposed values of {proposed} for {message.message_id} out of {len(self.gmembers)} members')
            if len(proposed) == len(self.gmembers): # If the sender has received proposals from all the processes
                self.requestorder(message.message_id, time) # Invoke the requestorder message to process and respond
        elif (type) == 'REPLAY_TO': # If the message has a type of REPLAY_TO, which is the sender telling itself that it needs to redo the request order:
            print(f'{self} is retrying order of {message.message_id} with proposed value of {message.proposed_id}')
            self.requestorder(message.message_id, time) # Invoke the requestorder method again, which has logic for redoing the request order
        pass



    def receive_message(self, frm, message, time): # On a message receive:
        print(f'Time {time}:: {self} RECEIVED message [{message.message_id}] of type [{message.mtype}] from {frm}')
        self.totalorder(frm, message, time) # Invoke the totalorder method


    def deliver_message(self, time, message):
        print(f'Time {time:4}:: {self} DELIVERED message [{message.message_id}] -- {message.payload}')
      

# Driver: you DO NOT need to change anything here
class Driver:
    def __init__(self, sim):
        self.hosts = []
        self.sim = sim

    def run(self, nhosts=3):
        for i in range(nhosts):
            host = Host(self.sim, i)
            self.hosts.append(host)
        
        for host in self.hosts:
            host.gmembers = self.hosts
            host.initialize()

        for event in multicast_events:
            time = event[0]
            message_id = event[1]
            message_type = 'DRIVER_MCAST'
            host_id = event[2]
            payload = event[3]
            self.sim.add_event(Event(time, self.hosts[host_id].multicast, time, message_id, message_type, payload))

def main():
    # Create simulation instance
    sim = Simulator(debug=False, random_seed=1233)

    # Start the driver and run for nhosts (should be >= 3)
    driver = Driver(sim)
    driver.run(nhosts=5)

    # Start the simulation
    sim.run()                 

if __name__ == "__main__":
    main()    