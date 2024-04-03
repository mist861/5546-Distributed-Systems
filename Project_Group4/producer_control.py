import pika, sys, os
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import threading
import uuid
import mysql.connector
import time
import re
import random
from datetime import datetime

credentials = pika.credentials.PlainCredentials('distributed_test', '*****', erase_on_connect=False)
host='172.26.160.1'
random.seed()
failstate = [1,2,3,4,5,6,7,8,9,10]

def insertSQL(table, ID, body, attempt, run):
    mysql_connection = mysql.connector.connect(host='172.26.160.1', database='distributed_producer', user='producer', password='******')
    mysql_check_query = ("""SELECT COUNT(*) FROM {table_name} WHERE Message_ID = %s""".format(table_name = table))
    mysql_insert_query = ("""INSERT INTO {table_name} (Message_ID, Message_Body, Attempt, Run_ID, Time) VALUES (%s,%s,%s,%s,%s)""".format(table_name = table))
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    cursor = mysql_connection.cursor()
    cursor.execute(mysql_check_query, (ID,))
    rowCount = cursor.fetchone()
    if rowCount[0] == 0:
        cursor.execute(mysql_insert_query, (ID, body, attempt, run, now))
        mysql_connection.commit()
        print(f' [X] Inserted message {ID} into MySQL {table} Table')
    elif rowCount[0] > 0:
        print (f' [W] Message {ID} is already in the MySQL {table} Table')
    cursor.close()
    if mysql_connection.is_connected():
        mysql_connection.close()

def sendMQ(body, run, *args, **kwargs):
    if kwargs.get('ID') == None:
        ID = uuid.uuid4().hex
    else:
        ID = kwargs.get('ID')
    if kwargs.get('attempt') == None:
        attempt = 1
    else:
        attempt = kwargs.get('attempt')
    if attempt == 1:
        insertSQL('Sent', ID, body, attempt, run)
    failure = random.choice(failstate)
    if failure == 1:
        print(f'Failed to send MQ for message {ID} for run {run}')
    else:
        print(f"Sending message: {body} with ID: {ID} for run {run} and attempt {attempt}")
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials))
        channel = connection.channel()
        channel.basic_publish(exchange='', routing_key='distributed_test_service', body=f'{ID}_{body}_{attempt}_{run}')
        connection.close()
    return True

def main():
    if len(sys.argv) < 2:
        print('Usage: master.py <port>')
        sys.exit(0)
    port = int(sys.argv[1])
    server = SimpleXMLRPCServer(("localhost", port))
    print(f"Listening on port {port}...")
    server.register_function(sendMQ) #register registerworker()
    #server.register_function(readRedis)
    server.serve_forever()


if __name__ == '__main__':
    main()