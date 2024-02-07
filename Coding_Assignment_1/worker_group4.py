from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
import sys
import json

#Storage of data
data_table = {}
group = 'NaN'


def load_data(group):
    global data_table
    if group == 'am':
        with open('data-am.json') as data_json_am: #open am json
            data_table = json.load(data_json_am) #load am json into data_table dict
        data_json_am.close() #close am json
    elif group == 'nz':
        with open('data-nz.json') as data_json_nz: #open nz json
            data_table = json.load(data_json_nz) #load nz json into data_table dict
        data_json_nz.close() #close nz json
    pass

def getbyname(name):
    print(f'Request for getbyname({name}) received.') #info statement
    result_list = {} #initialize empty, local result dict
    load_data(group) #load the data on call to support future publishing
    for key, value in data_table.items(): #iterate through the JSON dict for keys and values
            if value.get('name') == name: #check each set of values under each main key for the key:value pair of 'name':'input_name'.  Since this is a nested dict, the "values" of the top level keys are also dicts, with their own keys and values.
                print('Results found!') #info statement
                result_list.update({key: json.dumps(value)}) #append the higher level block where the name was found, if found, to the result dict
    if not result_list:
        return {
            'error': True,
            'result': 'No users found with that name'
        }
    else:
        return {
            'error': False,
            'result': result_list
        }

def getbylocation(location):
    print(f'Request for getbylocation({location}) received.') #info statement
    result_list = {} #initialize empty, local result dict
    load_data(group) #load the data on call to support future publishing
    for key, value in data_table.items(): #iterate through the JSON dict for keys, values
            if value.get('location') == location: #check each set of values under each main key for the key:value pair of 'location':'input_location'
                print('Results found!') #info statement
                result_list.update({key: json.dumps(value)}) #append the higher level block where the name was found, if found, to the result dict
    if not result_list:
        return {
            'error': True,
            'result': 'No users found for that location'
        }
    else:
        return {
            'error': False,
            'result': result_list
        }

def getbyyear(location, year):
    print(f'Request for getbyyear({location}, {year}) received.') #info statement
    result_list = {} #initialize empty, local result dict
    load_data(group) #load the data on call to support future publishing
    for key, value in data_table.items(): #iterate through the JSON dict for keys, values
            if value.get('location') == location and value.get('year') == year: #check each set of values under each main key for the key:value pairs of 'location':'input_location' AND 'year':'input_year'
                print('Results found!') #info statement
                result_list.update({key: json.dumps(value)}) #append the higher level block where the name was found, if found, to the result dict
    if not result_list:
        return {
            'error': True,
            'result': 'No users found for that year in that location'
        }
    else:
        return {
            'error': False,
            'result': result_list
        }

def registerworker(worker_name, worker_host, port, master):
    global group #load the global group var, to track which file to use
    try:
        group = ServerProxy(f'http://{master}/').registerworker(worker_name, worker_host, port) #call the master server, which will add the worker to the workers dict and the group array of its choosing and return the group name, which is stored in the global var
    except:
        print('Unable to connect to master!') #if there is a communication error:
        sys.exit(0) #exit
    if group != 'NaN': #If the group got updated:
        print(f'Worker group set to: {group}') #info statement
        print('Registered with master!') #info statment
    else:
        print('Worker group not set!') #if, for whatever reason, the rpc call was successufl but group still wasn't updated:
        sys.exit(0) #exit
    pass

def main():
    if len(sys.argv) < 3:
        print('Usage: worker.py <worker_name> <worker_hostname> <port> <master:port>')
        sys.exit(0)

    worker_name = str(sys.argv[1]) #get the worker_name
    worker_host = 'localhost'
    worker_host = str(sys.argv[2]) #get the worker_name, if provided
    port = int(sys.argv[3]) #get the worker port
    master = str(sys.argv[4]) #get the master:port
    registerworker(worker_name, worker_host, port, master) #register the server with the master, using the worker name and port.
    server = SimpleXMLRPCServer(("localhost", port))

    print(f"Listening on port {port}...")

    server.register_function(getbyname) #register getbyname()
    server.register_function(getbylocation) #register getbylocation()
    server.register_function(getbyyear) #register getbyyear()
    server.serve_forever()

if __name__ == '__main__':
    main()