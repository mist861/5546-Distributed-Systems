import pika, sys, os
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import threading
import uuid
import redis
import mysql.connector
import time
import re
import random
from datetime import datetime
import gc
from multiprocessing import Process

gc.enable() # We're using garbage collection since there are multiple threads, but honestly I'm not sure it's doing a whole lot
credentials = pika.credentials.PlainCredentials('distributed_test', '*****', erase_on_connect=False) # Set the RabbitMQ credentials
host='172.26.160.1' # Set the RabbitMQ host
random.seed() # Initilize random
failstate = [1,2,3,4,5,6,7,8,9,10] # Create an array of choices for random to later pull from

def flagRedis(key, value): # Method to set a retry flag in Redis
    redisCon = redis.Redis(host='172.26.160.1', port='6379', username='producer', password='*****', decode_responses=True) # Define the Redis connection
    result = redisCon.get(key) # Check if the flag is already set in Redis
    if result == value: # If it is:
        print(f' [W] {key}:{value} is already in Redis')
        return False
    else: # If it isn't
        redisCon.set(f'{key}', f'{value}') # Set the flag
        print(f' [X] Inserted {key}:{value} into Redis')
        return True

def writeRedis(ID, body, attempt, run): # Method to write messages to Redis
    redisCon = redis.Redis(host='172.26.160.1', port='6379', username='producer', password='*****')
    redisCon.set(f'sent_{ID}_{attempt}_{run}', f'{body}') # Note that all the important metadata bits are stored in the key, separated by _, but with "sent" as the first chunk so that we can find it later
    print(f' [X] Inserted message {ID} into Redis')
    pass

def readRedisKeys(): # Method to read key IDs from Redis, used to check if there is anything to retry
    redisCon = redis.Redis(host='172.26.160.1', port='6379', username='producer', password='*****', decode_responses=True)
    keys = list(redisCon.scan_iter('sent_*')) # This iterates all the keys and stores them in the list, if any
    return keys

def readRedis(key): # Method to read full values from Redis with given key
    redisCon = redis.Redis(host='172.26.160.1', port='6379', username='producer', password='*****', decode_responses=True)
    if redisCon.exists(key) > 0: # First make sure the key exists in redis and wasn't removed
        result = redisCon.getdel(key) # Retrieve AND DELETE the value from Redis, so that it's not retried later (until it's put back by sendMQ)
    else:
        result = None
    return result

def readSQLID(table): # Reads the IDs of messages from the given MySQL table, to see if there is anything that needs to be retried
    mysql_connection = mysql.connector.connect(host='172.26.160.1', database='distributed_producer', user='producer', password='*****') # Define the MySQL connection
    mysql_check_query = ("""SELECT COUNT(*) FROM {table_name} """.format(table_name = table)) # Define the MySQL check query, so see if there is anything
    mysql_select_query = ("""SELECT Message_ID FROM {table_name} """.format(table_name = table)) # Define the MySQL select query
    
    cursor = mysql_connection.cursor() # Initialize a MySQL cursor
    cursor.execute(mysql_check_query) # Execute the check query
    rowCount = cursor.fetchone() # Retrieve the rowcound
    if rowCount[0] == 0: # If there are no IDs in the MySQL table (if rowCount is 0)
        print(f' [A] No messages found in MySQL {table} Table')
        failed = None # Set "failed" to None, because there are none that are failed
    elif rowCount[0] > 0: # If there are IDs in the MySQL table
        print (f' [A] {rowCount} messages found in MySQL {table} Table')
        cursor.execute(mysql_select_query) # Fetch all the IDs
        failed = cursor.fetchall() # Store them in failed
    cursor.close() # Close the cursor
    if mysql_connection.is_connected(): # If MySQL is still connected:
        mysql_connection.close() # Close the connection. NOTE: this isn't suuuuuper great, it's better if connections are long-lived, but I don't want to maintain sessions
    return failed, rowCount # Return the tuple of failed IDs and the number

def readSQL(table, ID): # Method to read full values from the given MySQL table for a given ID.  This is called AFTER readSQLID, so we know by now if there are values or not, so no mysql_check_query
    mysql_connection = mysql.connector.connect(host='172.26.160.1', database='distributed_producer', user='producer', password='Pr0ducerP8ss')
    mysql_select_query = ("""SELECT * FROM {table_name} WHERE Message_ID = %s""".format(table_name = table)) # Note the {table_name} and %s: values can be given as parameters, but table names cannot, so the former can be %s passed later while the table name has to be a string variable passed now
    
    cursor = mysql_connection.cursor()
    cursor.execute(mysql_select_query, (ID,)) # This is where the %s comes in
    failed = cursor.fetchone() # Since we're only retrieving for one ID (to try and keep down the size of this variable), we only fetchone instead of fetchall
    cursor.close()
    if mysql_connection.is_connected():
        mysql_connection.close()
    return failed # Return the tuple of the single fetched object

def insertSQL(table, ID, body, attempt, run): # Method to insert a given message into the given MySQL table
    mysql_connection = mysql.connector.connect(host='172.26.160.1', database='distributed_producer', user='producer', password='Pr0ducerP8ss')
    mysql_check_query = ("""SELECT COUNT(*) FROM {table_name} WHERE Message_ID = %s""".format(table_name = table))
    mysql_insert_query = ("""INSERT INTO {table_name} (Message_ID, Message_Body, Attempt, Run_ID, Time) VALUES (%s,%s,%s,%s,%s)""".format(table_name = table)) # Note that this has a lot more %s, order is important
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f') # Set the datetime (to compare with the datetime in the message body)
    cursor = mysql_connection.cursor()
    cursor.execute(mysql_check_query, (ID,))
    rowCount = cursor.fetchone()
    if rowCount[0] == 0: # These are opposite to the above, if any results (rowcount > 0), do NOT continue
        cursor.execute(mysql_insert_query, (ID, body, attempt, run, now)) # This is where all the %s come in
        mysql_connection.commit() # This is a MySQL thing, all changes (writes/modifies/deletes) must be committed to take effect
        print(f' [X] Inserted message {ID} into MySQL {table} Table')
    elif rowCount[0] > 0:
        print (f' [W] Message {ID} is already in the MySQL {table} Table')
    cursor.close()
    if mysql_connection.is_connected():
        mysql_connection.close()

def deleteSQL(table, ID): # Method to remove a message with given ID from a given table (pretty much always "Failed")
    mysql_connection = mysql.connector.connect(host='172.26.160.1', database='distributed_producer', user='producer', password='Pr0ducerP8ss')
    mySql_delete_query = ("""DELETE FROM {table_name} WHERE Message_ID = %s;""".format(table_name = table))

    cursor = mysql_connection.cursor()
    cursor.execute(mySql_delete_query, (ID,))
    mysql_connection.commit()
    print(f' [X] Deleted message {ID} from MySQL {table} Table')
    cursor.close()
    if mysql_connection.is_connected():
        mysql_connection.close()

def popRedis(ID): # Method to remove message with a given ID from Redis, unlike readRedis above which reads then deletes
    redisCon = redis.Redis(host='172.26.160.1', port='6379', username='producer', password='Pr0ducerP8ss')
    for key in redisCon.scan_iter(f'sent_{ID}*'): # First we get the full key name (all keys are stored with _ separating important key bits, INCLUDING ATTEMPT NUMBER)
        redisCon.delete(key) # Then we delete all the found key:value pairs.  Which SHOULD only be 1, but since the keys are stored with things like ATTEMPT NUMBER we're being safe
    pass

def retryRedis(): # Method to retry messages in Redis (that haven't been ACK'd)
    while True: # So forever:
        lock = flagRedis('retry', 'True') # Try to set the retry flag in Redis.  This will return True (if set successfully) or False (if it is already set or is otherwise set unsuccessfully)
        if lock: # If lock is True, meaning we set the flag to True and "have" the lock
            keys = readRedisKeys() # Get all the keys from anything in Redis
            if any(True for _ in keys): # If there are any keys
                for key in keys: # For each key
                    result = readRedis(key) # Set result to the retrieved value.  REMEMBER THAT THIS IS A getdel SO IT ALSO REMOVES IT FROM THE CACHE
                    if result != None:
                        meta = re.split(r'_', key) # Split the key up by _
                        ID = meta[1] # Set the second chunk to ID
                        attempt = int(meta[2]) # Set the third chunk to attempt 
                        run = int(meta[3]) # Set the fourth chunk to run.  If you're wondering about the first chunk: it's literally "sent".  Which is set below and just used to later retrieve the keys
                        if (attempt % 3) != 0: # If the LAST attempt is not divisible by 3 (so if it ran 1 time, 2 times, 4 times, etc.)
                            attempt = attempt + 1 # Increase the attempt by 1 (so if it ran 2 times, this becomes attempt 3)
                            print(f'Retrying message {ID} for run {run} for attempt {attempt}')
                            sendMQ(body=result, run=run, ID=ID, attempt=attempt) # Invoke sendMQ below to publish the message to the RabbitMQ service queue
                        elif(attempt % 3) == 0: # If the LAST attempt is divisible by 3 (so if it failed 3 times, 6 times, 9 times, etc.)
                            print(f'Message {ID} has been unsuccessfully retried {attempt} times.  Writing to MySQL Failed table.')
                            insertSQL('Failed', ID, result, attempt, run) # Do not run it, instead move it to the MySQL Failed table
                    elif result == None: # If the key was removed mid-retry (by an ACK), then:
                        print(f'Message with ID {key} already removed from Redis')
            else: # If there are no keys to retry:
                print('No messages to retry in Redis!')
            flagRedis('retry', 'False') # Once it's all done, set the retry flag back to False
        elif not lock: # If lock is False, meaning we didn't set the retry flag successfully
            print('Another thread is currently retrying, not trying Redis')
        time.sleep(60)
    pass

def retryFailed(): # Like the above, but for MySQL
    while True:
        lock = flagRedis('retry', 'True')
        if lock:
            keys, count = readSQLID('Failed') # Get the list of keys, if any, from the Failed table
            if keys: # If keys exists (if there are any keys)
                print(f'Found {count} failed messages in MySQL!')
                for key in keys: # For eack key
                    key = key[0] # Pull the actual message ID
                    print(f'Retrying message {key} from MySQL Failed table')
                    result = readSQL('Failed', key) # Retrieve the full message in the Failed table with that Message ID
                    if len(result) > 1: # If the key is still in SQL and pulls real results
                        body = result[1] # Set the body to the second chunk
                        attempt = (result[2] + 1) # Set the attempt to the third chunk and increment by 1
                        run = result[3] # Set the run ID to the fourth chunk.  If you're wondering about the first chunk: it's the message ID!  Which we already have in key!
                        sendMQ(body=body, ID=key, attempt = attempt, run=run) # Invoke sendMQ to publish the failed message to the RabbitMQ service queue
                        deleteSQL('Failed', key) # REMOVE THE MESSAGE FROM THE Failed TABLE
                    else: # If the key was removed from the Failed table mid-retry (like by an ACK), then:
                        print(f'Message with ID {key} already removed from the SQL Failed table')
            elif not keys: # If no keys are found:
                print('No messages to retry in MySQL!')
            flagRedis('retry', 'False') # Once it's all done, set the retry flag back to False
        elif not lock:
            print('Another thread is currently retrying, not trying MySQL')
        time.sleep(175)
    pass

def sendMQ(body, run, *args, **kwargs): # Method to publish messages to the RabbitMQ service queue
    if kwargs.get('ID') == None: # If an ID isn't given:
        ID = uuid.uuid4().hex # Use uuid to generate a new, (hopefully) unique ID
    else: # But if an ID IS given:
        ID = kwargs.get('ID') # Use it
    if kwargs.get('attempt') == None: # If attempt isn't given:
        attempt = 1 # Then this is the first attempt
    else: # But if attempt is given:
        attempt = kwargs.get('attempt') # Use it!
    writeRedis(ID, body, attempt, run) # Write the message to Redis
    if attempt == 1: # If this is the first attempt:
        insertSQL('Sent', ID, body, attempt, run) # Write the message to the MySQL Sent table, NOT FAILED, for tracking
    failure = random.choice(failstate) # Have random randomly select an int from the failstate array
    if failure == 1: # If random chose 1 (a 1/10 chance)
        print(f'Failed to send MQ for message {ID} for run {run}') # Do nothing
    else: # If random chose anything else (a 9/10 chance)
        print(f"Sending message: {body} with ID: {ID} for run {run} and attempt {attempt}")
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials)) # Define the RabbitMQ connection
        channel = connection.channel() # Define the RabbitMQ channel
        channel.basic_publish(exchange='', routing_key='distributed_test_service', body=f'{ID}_{body}_{attempt}_{run}') # Publish the message to the RabbitMQ channel, storing everything in the message separated by _
        connection.close() # Close the RabbitMQ connection.  NOTE: like above, long-lived connections are better
    return True

def callback(ch, method, properties, body): # Method to handle response to messages received (here, from the acknowledgement queue)
    ID = body.decode() # First decode the body.  Which in this case, is just the message ID
    failure = random.choice(failstate) # Introduce random failure, like above!
    if failure == 1:
        print(f'Failed to receive ACK for message {ID}')
    else:
        print(f" [x] Received ACK for message {ID}")
        popRedis(ID) # Delete the message from Redis, so that it is not retried (if it exists)
        deleteSQL('Failed', ID) # Delete the message from MySQL, so that it is not retried (if it exists)
        print(f' [X] Removed message {ID} from Redis and MySQL')
    pass
#        insertSQL('ACK', ID, 'ACK', 0, 0)

def receiveMQ(): # Method to watch the acknowledgement queue
    while True:
        ackconnection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials))
        ackchannel = ackconnection.channel()
        ackchannel.basic_consume(queue='distributed_test_ack', on_message_callback=callback, auto_ack=True) # Define the queue AND what to do if any messages are received, in this case the callback() method
        print(' [*] Waiting for ACK. To exit press CTRL+C')
        try:
            ackchannel.start_consuming() # BlockConnection likes to kinda... Die.  So we're running this in a try so that we can try and restart if it does (it kinda works)
        except:
            print('RabbitMQ connection failed, restarting...')
            ackchannel.stop_consuming() # If it does die, stop consuming
            ackchannel.close() # And close the connection.

def main():
    if len(sys.argv) < 2: # Unlike consumer.py, this does require arguments.  Just the port number on which it should serve sendMQ()
        print('Usage: master.py <port>')
        sys.exit(0)
    port = int(sys.argv[1]) # Set the port number
    server = SimpleXMLRPCServer(("localhost", port)) # Define the SimpleXMLRPCServer
    print(f"Listening on port {port}...")
    server.register_function(sendMQ) # Register sendMQ()
    processes = [] # Define an array to store process names
    flagRedis('retry', 'False') # Set the retry flag to false.  NOTE: If multiple producer.py's are started at once, this COULD get funky if one starts at the same time that another tries to do another retry.  This isn't the best flag method.
    for p in range (1,4): # We're starting up 3 whole processes for receiveMQ()!
        mqProcess = Process(target=receiveMQ, args=()) # Specify which method will run in the process
        mqProcess.daemon = True # Set it to be a daemon of main so that we can control it
        mqProcess.start() # Start the process
        processes.append(mqProcess) # Add the process to the process array, for tracking.  ...Which we're not really using here, but would be quite nice for error handling.
    retryThread = threading.Thread(target=retryRedis, args=()) # Define a thread to run retryRedis
    try:
        retryThread.start() # Start the retryRedis thread
    except:
        print('Redis retry thread failed')
        exit(1)
    failedThread = threading.Thread(target=retryFailed, args=()) # Define a thread to run retryFailed
    try:
        failedThread.start() # Start the retryFailed thread.  Note that in Python, all "threads" are single-threaded with main.  So they can drown each other out.  That's fine for the retries and initial sends, but less fine for receiveMQ's ACKs, so that method gets to be multiprocessed
    except:
        print('MySQL retry thread failed')
        exit(1)
    server.serve_forever() # Start serving sendMQ via RPC


if __name__ == '__main__':
    main()