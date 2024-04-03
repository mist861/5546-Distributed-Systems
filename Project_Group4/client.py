import xmlrpc.client
import sys
from datetime import datetime

producer_host = int(sys.argv[1]) # Set the producer's host
producer_port = int(sys.argv[2]) # Set the producer's port
iterations = (int(sys.argv[3]) + 1) # Set the number of messages to send.  Since arrays start at 0, we add 1
run = int(sys.argv[4]) # Set an ID to track the current run

with xmlrpc.client.ServerProxy(f"http://{producer_host}:{producer_port}/") as proxy:
    for i in range(1, iterations): # For all of the number of messages (iterations) to send:
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f') # Set the time, down to the millisecond
        body = f'Iteration {i} time {now}' # Set the body
        print(f'Client => Sending MQ message {i} with body: {body}')
        result = proxy.sendMQ(body, run) # Invoke the sendMQ message in producer.py
        if result: 
            print(f'Successfully sent MQ message {i} with body: {body}')
        elif not result:
            print(f'Failed to send MQ message {i} with body: {body}')
    pass
