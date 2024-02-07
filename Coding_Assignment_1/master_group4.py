from curses.ascii import isdigit
from decimal import localcontext
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import sys
import re #for performing regex matches, used to select nodes based on name
import html #for sanitizing input
from itertools import cycle


workers = {} #global master dict for keeping track of workers

def iterate_workers():
    for key, value in cycle(workers.items()):
        yield key, value

workers_iteration = iterate_workers()

def round_robin():
    key, value = next(workers_iteration)
    return [key, value]

def registerworker(worker_name, worker_host, port):
    global workers, workers_iteration #load the global vars
    workers.update({f'{worker_name}': ServerProxy(f'http://{worker_host}:{port}')}) #append/update the worker to the master dict
    workers_iteration = iterate_workers() #reiterate workers to pick up any new workers
    print(f'{worker_name} registered!') #info statement
    print(f'Current workers: {workers}') #info statement
    return 'Success' #return success, so that the worker knows it was registered

def deregisterworker(worker_name):
    global workers, workers_iteration #load the global vars
    workers.pop(worker_name) #append/update the worker to the master dict
    workers_iteration = iterate_workers() #reiterate workers to drop any new workers
    print(f'{worker_name} deregistered!') #info statement
    print(f'Current workers: {workers}') #info statement
    return 'Success' #return the group name to the worker, so that it knows which file to use
      
def getbylocation(location):
    location = html.escape(location) #sanitize the user input
    print(f'Request for getbylocation({location}) received.') #info statement after sanitizing
    result = {} #init empty dict to temporarily store results
    result_list = {} #init empty dict to store combined results
    error = False #init bool to track if there were any errors, default to False becasue no errors are expected
    for i in range(5):
        worker = round_robin()
        print(f'Sending request to {worker[0]}!')
        try:
            result.update(worker[1].getbylocation(location)) #get results (if any) from rpc call to worker-1, stored in result
            if result.get('error') == False: #if the rpc call succeeds and doesn't return an error (since result is a dict with a key "error" with possible values "True" and False"), do:
                print(f'Results found in {worker[0]}!') #info statement
                result_list.update(result.get('result')) #append the result (which is in the form of a dict) to the result_list dict
        except:
            print(f'There was an issue calling {worker[0]}')
            deregisterworker(worker[0]) #deregister the failed worker
            error = True
        else:
            error = False
            break
    if not result_list and error == False: #if the result_list dict is still empty, then:
        return {
            'error': error,
            'result': 'No users were found for that location.'
        }
    elif error == True:
        return {
            'error': error,
            'result': 'There was an error in the system.'
        }
    else:
        return {
            'error': error,
            'result': result_list
    }

def getbyname(name):
    name = html.escape(name) #sanitize the user input
    print(f'Request for getbyname({name}) received.') #info statement after sanitizing
    result = {} #init empty dict to temporarily store results
    result_list = {} #init empty dict to store combined results
    error = False #init bool to track if there were any errors, default to False becasue no errors are expected
    for i in range(5):
        worker = round_robin()
        print(f'Sending request to {worker[0]}!')
        try:
            result.update(worker[1].getbyname(name)) #get results (if any) from rpc call to worker-1, stored in result
            if result.get('error') == False: #if the rpc call succeeds and doesn't return an error (since result is a dict with a key "error" with possible values "True" and False"), do:
                result_list.update(result.get('result')) #append the result (which is in the form of a dict) to the result_list dict
        except:
            print(f'There was an issue calling worker[0]!')
            deregisterworker(worker[0]) #deregister the failed worker
            error = True
        else:
            error = False
            break
    if not result_list and error == False: #if the result_list dict is still empty, then:
        return {
            'error': error,
            'result': 'No user found with that name'
        }
    elif error == True: #if the result_list dict is still empty AND error was set to True by the except, then:
        return {
            'error': error,
            'result': 'There was an error in the system.'
        }
    else: #if the result_list dict isn't empty, even if there was an error, then:
        return {
            'error': error,
            'result': result_list
    }

def getbyyear(location, year):
    location = html.escape(location) #sanitize the user input
    if isinstance(year, int): #make sure the year input is an integer
        print(f'Request for getbyyear({location}, {year}) received.') #info statement after ensuring year is an integer
        result = {} #init empty dict to temporarily store results
        result_list = {} #init empty dict to store combined results
        error = False #init bool to track if there were any errors, default to False becasue no errors are expected
        for i in range(5):
            worker = round_robin()
            print(f'Sending request to {worker[0]}!')   
            try:    
                result.update(worker[1].getbyyear(location, year)) #get results (if any) from rpc call to worker-1, stored in result
                if result.get('error') == False: #if the rpc call succeeds and doesn't return an error (since result is a dict with a key "error" with possible values "True" and False"), do:
                    print(f'Results found in {worker[0]}') #info statement
                    result_list.update(result.get('result')) #append the result (which is in the form of a dict) to the result_list dict
            except:
                print(f'There was an issue calling worker[0]!')
                deregisterworker(worker[0]) #deregister the failed worker
                error = True
            else:
                error = False
                break 
    if not result_list and error == False: #if the result_list dict is still empty, then:
        return {
            'error': error,
            'result': 'No users were found for that location.'
        }
    elif error == True:
        return {
            'error': error,
            'result': 'There was an error in the system.'
        }
    else:
        return {
            'error': error,
            'result': result_list
    }  

def main():
    port = int(sys.argv[1])
    server = SimpleXMLRPCServer(("localhost", port))
    print(f"Listening on port {port}...")


    server.register_function(registerworker) #register registerworker()
    server.register_function(getbyname) #register getbyname()
    server.register_function(getbylocation) #register getbylocation()
    server.register_function(getbyyear) #register getbyyear()
    server.serve_forever()


if __name__ == '__main__':
    main()