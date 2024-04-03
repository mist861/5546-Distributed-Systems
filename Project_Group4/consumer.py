import pika, sys, os, mysql.connector, re, random
from datetime import datetime

credentials = pika.credentials.PlainCredentials('distributed_test', '*****', erase_on_connect=False) # Set the RabbitMQ credentials
host = '172.26.160.1' # Set the RabbitMQ host
random.seed() # Initilize random
failstate = [1,2,3,4,5,6,7,8,9,10] # Create an array of choices for random to later pull from

def insertSQL(table, ID, body, attempt, run): # Method to insert messages into MySQL
    mysql_connection = mysql.connector.connect(host='172.26.160.1', database='distributed_consumer', user='consumer', password='******') # Define the MySQL connection
    mysql_check_query = ("""SELECT COUNT(*) FROM {table_name} WHERE Message_ID = %s""".format(table_name = table)) # Define the MySQL check query, which will see if any results exist for a given ID
    mysql_insert_query = ("""INSERT INTO {table_name} (Message_ID, Message_Body, Attempt, Run_ID, Time) VALUES (%s,%s,%s,%s,%s)""".format(table_name = table)) # Define the MySQL insert query
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f') # Set the datetime (to be compared with the send datatime)
    cursor = mysql_connection.cursor() # Initialize a MySQL cursor
    cursor.execute(mysql_check_query, (ID,)) # Run the check query
    rowCount = cursor.fetchone() # Get the rowcount
    if rowCount[0] == 0: # If the ID isn't found in the database (rowcount is 0)
        cursor.execute(mysql_insert_query, (ID, body, attempt, run, now)) # Run the insert query
        mysql_connection.commit() # Commit the insert query (this is a SQL database thing)
        print(f' [X] Inserted message {ID} into MySQL {table} Table')
    elif rowCount[0] > 0: # If the ID IS in the database (rowcound > 0)
        print (f' [W] Message {ID} is already in the MySQL {table} Table')
    cursor.close() # Close the MySQL cursor
    if mysql_connection.is_connected(): # If MySQL is still connected
        mysql_connection.close() # Close the connection. NOTE: this isn't suuuuuper great, it's better if connections are long-lived, but I don't want to maintain sessions

def sendACK(ID): # Method to publish ACK to RabbitMQ
    failure = random.choice(failstate) # Let random randomly choose from the failstate array
    if failure == 1: # If random chooses 1 (a 1/10 chance)
        print(f'Failed to send ACK for message {ID}') # Print "I failed" and do nothing else
        pass  # And do nothing else
    else: # If random chooses anything else (9/10 chance)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials)) # Define the RabbitMQ connection
        channel = connection.channel() # Define the RabbitMQ channel
        channel.basic_publish(exchange='', routing_key='distributed_test_ack', body=ID) # Publish the ack to the ACK queue.  Note that here, the body is JUST the message ID
        print("Sending message: ACK")
        connection.close() # Close the RabbitMQ connection.  NOTE: like above, long-lived connections are better
        pass

def callback(ch, method, properties, body): # Method to handle message receives.  NOTE: callback is a default RabbitMQ method that requires these 4 arguments, even if we're not using 3 of them
    failure = random.choice(failstate) # Do the random failure like above
    message = body.decode() # Get the message body
    message = re.split(r'_', message) # Split the message body up (RabbitMQ requies everything be in one block, we're using "_" as a separator here)
    ID = message[0] # Set ID to the first chunk
    body = message[1] # Set body to the second chunk
    attempt = message[2] # Set attempt tot he third chunk
    run = message[3] # Set run ID to the fourth chunk
    if failure == 1: # Same as the random failure above
        print(f'Failed to consume message {ID}')
    else:
        print(f" [x] Received {body} for run {run} for attempt {attempt}")
        insertSQL('Received', ID, body, attempt, run) # Invoke insertSQL to insert the message into MySQL
        print("Sending ACK")
        sendACK(ID) # Invoke sendACK to send the ack back to producer.py
    pass

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials)) # Define RabbitMQ connection, which is separate from the above in sendACK
    channel = connection.channel()

    channel.basic_consume(queue='distributed_test_service', on_message_callback=callback, auto_ack=True) # See, this time we're consuming the service queue, not the ACK queue

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming() # Begin consuming, indefinetly.

if __name__ == '__main__': # Run everything
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)