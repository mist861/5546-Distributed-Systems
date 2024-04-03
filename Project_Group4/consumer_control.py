import pika, sys, os, mysql.connector, re, random
from datetime import datetime

credentials = pika.credentials.PlainCredentials('distributed_test', 'd1stP8ss', erase_on_connect=False)
host = '172.26.160.1'
random.seed()
failstate = [1,2,3,4,5,6,7,8,9,10]

def insertSQL(table, ID, body, attempt, run):
    mysql_connection = mysql.connector.connect(host='172.26.160.1', database='distributed_consumer', user='consumer', password='C0nsumerP8ss')
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

def callback(ch, method, properties, body):
    failure = random.choice(failstate)
    message = body.decode()
    message = re.split(r'_', message)
    ID = message[0]
    body = message[1]
    attempt = message[2]
    run = message[3]
    if failure == 1:
        print(f'Failed to consume message {ID}')
        pass
    else:
        print(f" [x] Received {body} for run {run} for attempt {attempt}")
        insertSQL('Received', ID, body, attempt, run)
        pass

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials))
    channel = connection.channel()

    channel.basic_consume(queue='distributed_test_service', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)