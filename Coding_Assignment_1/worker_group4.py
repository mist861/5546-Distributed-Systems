from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
import sys
import json
import re #for performing regex matches, used to parse arguments

#Storage of data
data_table = {} #global dict to store the data
group = 'NaN' #null variable to store group status, this is only needed because both workers run on the same box


def load_data(group):
    global data_table
    with open(f'data-complete-{group}.json') as data_json: #open the json with the defined group
            data_table = json.load(data_json) #load json into data_table dict
    pass

def getbyname(name):
    print(f'Request for getbyname({name}) received.') #info statement
    result_list = {} #initialize empty, local result dict
    for key, value in data_table.items(): #iterate through the JSON dict for keys and values
            if value.get('name') == name: #check each set of values under each main key for the key:value pair of 'name':'input_name'.  Since this is a nested dict, the "values" of the top level keys are also dicts, with their own keys and values.
                print('Results found!') #info statement
                result_list.update({key: json.dumps(value)}) #append the higher level block where the name was found, if found, to the result dict
    if not result_list:
        print('No results found!') #info statement
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
    for key, value in data_table.items(): #iterate through the JSON dict for keys, values
            if value.get('location') == location: #check each set of values under each main key for the key:value pair of 'location':'input_location'
                print('Results found!') #info statement
                result_list.update({key: json.dumps(value)}) #append the higher level block where the name was found, if found, to the result dict
    if not result_list:
        print('No results found!') #info statement
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
    for key, value in data_table.items(): #iterate through the JSON dict for keys, values
            if value.get('location') == location and value.get('year') == year: #check each set of values under each main key for the key:value pairs of 'location':'input_location' AND 'year':'input_year'
                print('Results found!') #info statement
                result_list.update({key: json.dumps(value)}) #append the higher level block where the name was found, if found, to the result dict
    if not result_list:
        print('No results found!') #info statement
        return {
            'error': True,
            'result': 'No users found for that year in that location'
        }
    else:
        return {
            'error': False,
            'result': result_list
        }

def registerworker(worker_name, worker, master):
    try:
        register = ServerProxy(f'http://{master}/').registerworker(worker_name, worker) #call the master server, which will add the worker to the workers dict and the group array of its choosing and return the group name, which is stored in the global var
    except:
        print('Unable to connect to master!') #if there is a communication error:
        sys.exit(0) #exit
    if register == 'Success': #If the rpc call returned 'Success'
        print('Registered with master!') #info statment
    else:
        print('Unable to register with master!') #if, for whatever reason, the rpc call was successufl but group still wasn't updated:
        sys.exit(0) #exit
    pass

def main():
    if len(sys.argv) < 5:
        print('Usage: worker.py <worker_name> <worker_hostname:port> <master:port> <group>')
        sys.exit(0)

    global group
    group = str(sys.argv[4]) #again, this wouldn't be needed if these workers ran on separate hosts and could have the same file name (and still have separate files)
    worker_name = str(sys.argv[1]) #get the worker name
    worker = str(sys.argv[2]) #get the worker host:port
    port = int(re.search(r'(?<=:)[0-9]*$', str(sys.argv[2])).group(0)) #parse the worker port with regex and turn the match into an int
    master = str(sys.argv[3]) #get the master:port, for simplicity's sake we're having this be one arg
    try:
            load_data(group) #load the data on call to support future publishing
    except:
            print('Please provide a valid group for the file data-complete-<group>.json.')
            exit(0)
    registerworker(worker_name, worker, master) #register the server with the master, using the worker name and port.
    server = SimpleXMLRPCServer(("localhost", port))

    print(f"Listening on port {port}...")

    server.register_function(getbyname) #register getbyname()
    server.register_function(getbylocation) #register getbylocation()
    server.register_function(getbyyear) #register getbyyear()
    server.serve_forever()

if __name__ == '__main__':
    main()