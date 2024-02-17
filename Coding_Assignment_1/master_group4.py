from curses.ascii import isdigit
from decimal import localcontext
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import sys
import html #for sanitizing input
from itertools import cycle #this will be used by the round robin cycle
import json #needed here as we're storing the registered workers in a json


workers = {} #global master dict for keeping track of workers

def load_workers(): #this is to load the workers file, if there is one, in the event of a master crash
    global workers
    try:
        with open('workers.json') as workers_json: #open workers json
            workers = json.load(workers_json) #load workers json into workers dict, if it exists
            print(f'Current workers: {workers}') #info statement
        pass
    except:
        print('No workers.json found!') #if there is no workers file, that's fine, the master just can't serve anything until at least one worker registers
        pass

def iterate_workers(): #creating a cycle to keep track of which workers we've used
    for key, value in cycle(workers.items()):
        yield key, value

workers_iteration = iterate_workers() #storing the current value of the cycle in a global var

def round_robin(): #every time this is called, the global var is moved to the next iteration of the cycle
    key, value = next(workers_iteration)
    return [key, value]

def registerworker(worker_name, worker): #this lets workers register with the master instead of being static
    global workers, workers_iteration #load the global vars
    workers.update({f'{worker_name}': f'{worker}'}) #append/update the worker to the master dict
    workers_iteration = iterate_workers() #reiterate workers to add any new workers to load
    print(f'{worker_name} registered!') #info statement
    print(f'Current workers: {workers}') #info statement
    with open('workers.json', 'w', encoding='utf-8') as workers_file:
        json.dump(workers, workers_file, ensure_ascii=False, indent=4) #write the workers to a json file, so that if the master crashes it can come back and serve without forcing all the workers to re-register
    return 'Success' #return success, so that the worker knows it was registered

def deregisterworker(worker_name): #unregister workers in the event that the master has issues calling them
    global workers, workers_iteration #load the global vars
    workers.pop(worker_name) #append/update the worker to the master dict
    workers_iteration = iterate_workers() #reiterate workers to drop any new workers from load
    print(f'{worker_name} deregistered!') #info statement
    print(f'Current workers: {workers}') #info statement
    with open('workers.json', 'w', encoding='utf-8') as workers_file: #overwrite the workers json file with the cleaned workers dict, effectively removing the unregistered workers from the file
        json.dump(workers, workers_file, ensure_ascii=False, indent=4)
    return 'Success' #return the group name to the worker, so that it knows which file to use
      
def getbylocation(location):
    location = html.escape(location) #sanitize the user input
    print(f'Request for getbylocation({location}) received.') #info statement after sanitizing
    result = {} #init empty dict to temporarily store results
    result_list = {} #init empty dict to store combined results
    error = False #init bool to track if there were any errors, default to False becasue no errors are expected
    for i in range(5): #attempt this five times before erroring out, which allows retries but with a limit
        try:
            worker = round_robin() #call the round_robin function to set workers to the next iteration of the workers_iteration cycle
            print(f'Sending request to {worker[0]}!') #info statement
            result.update(ServerProxy(f"http://{worker[1]}/").getbylocation(location)) #get results (if any) from rpc call to worker-1, stored in result
            if result.get('error') == False: #if the rpc call succeeds and doesn't return an error (since result is a dict with a key "error" with possible values "True" and False"), do:
                print(f'Results found in {worker[0]}!') #info statement
                result_list.update(result.get('result')) #append the result (which is in the form of a dict) to the result_list dict
        except:
            error = True #set error to True, to be used in logic below
            if workers:
                print(f'There was an issue calling {worker[0]}') #info statement, we're going to stop commenting on these now
                deregisterworker(worker[0]) #deregister the failed worker, IFF there are any workers remaining.  Without this, the except would fail, resulting in an unhandled exception.
            else:
                print('There are no available workers!')
        else:
            error = False #if at least one of the five tries is successful, make sure error is set to False
            break #if at least one of the five tries is successful, break the loop and continue
    if not result_list and error == False: #if the result_list dict is still empty after five tries but there were no errors, then:
        return {
            'error': error,
            'result': 'No users were found for that location.'
        }
    elif error == True: #if it loops five times and error stayed True, then:
        return {
            'error': error,
            'result': 'There was an error in the system.'
        }
    else: #if the result_list has results and error is False, return as normal:
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
    for i in range(5): #attempt this five times before erroring out, which allows retries but with a limit
        try:
            worker = round_robin() #call the round_robin function to set workers to the next iteration of the workers_iteration cycle
            print(f'Sending request to {worker[0]}!')
            result.update(ServerProxy(f"http://{worker[1]}/").getbyname(name)) #get results (if any) from rpc call to worker-1, stored in result
            if result.get('error') == False: #if the rpc call succeeds and doesn't return an error (since result is a dict with a key "error" with possible values "True" and False"), do:
                result_list.update(result.get('result')) #append the result (which is in the form of a dict) to the result_list dict
        except:
            error = True #set error to True, to be used in logic below
            if workers:
                print(f'There was an issue calling {worker[0]}')
                deregisterworker(worker[0]) #deregister the failed worker, IFF there are any workers remaining.  Without this, the except would fail, resulting in an unhandled exception.
            else:
                print('There are no available workers!')
        else:
            error = False #if at least one of the five tries is successful, make sure error is set to False
            break #if at least one of the five tries is successful, break the loop and continue
    if not result_list and error == False: #if the result_list dict is still empty after five tries but there were no errors, then:
        return {
            'error': error,
            'result': 'No user found with that name'
        }
    elif error == True: #if it loops five times and error stayed True, then:
        return {
            'error': error,
            'result': 'There was an error in the system.'
        }
    else: #if the result_list has results and error is False, return as normal:
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
            try:   
                worker = round_robin() #call the round_robin function to set workers to the next iteration of the workers_iteration cycle
                print(f'Sending request to {worker[0]}!') 
                result.update(ServerProxy(f"http://{worker[1]}/").getbyyear(location, year)) #get results (if any) from rpc call to worker-1, stored in result
                if result.get('error') == False: #if the rpc call succeeds and doesn't return an error (since result is a dict with a key "error" with possible values "True" and False"), do:
                    print(f'Results found in {worker[0]}') #info statement
                    result_list.update(result.get('result')) #append the result (which is in the form of a dict) to the result_list dict
            except:
                error = True #set error to True, to be used in logic below
                if workers:
                    print(f'There was an issue calling {worker[0]}')
                    deregisterworker(worker[0]) #deregister the failed worker, IFF there are any workers remaining.  Without this, the except would fail, resulting in an unhandled exception.
                else:
                    print('There are no available workers!')
            else:
                error = False #if at least one of the five tries is successful, make sure error is set to False
                break #if at least one of the five tries is successful, break the loop and continue
    else:
        return {
            'error': True,
            'result': 'Year must be an integer.'
        }
    if not result_list and error == False: #if the result_list dict is still empty after five tries but there were no errors, then:
        return {
            'error': error,
            'result': 'No users were found for that location during that year.'
        }
    elif error == True: #if it loops five times and error stayed True, then:
        return {
            'error': error,
            'result': 'There was an error in the system.'
        }
    else: #if the result_list has results and error is False, return as normal:
        return {
            'error': error,
            'result': result_list
    }  

def main():
    if len(sys.argv) < 2:
        print('Usage: master.py <port>')
        sys.exit(0)
    port = int(sys.argv[1])
    server = SimpleXMLRPCServer(("localhost", port))
    load_workers() #loading any workers in the event that the master crashes but the workers stayed registered and running, so that workers don't have to re-register
    print(f"Listening on port {port}...")


    server.register_function(registerworker) #register registerworker()
    server.register_function(getbyname) #register getbyname()
    server.register_function(getbylocation) #register getbylocation()
    server.register_function(getbyyear) #register getbyyear()
    server.serve_forever()


if __name__ == '__main__':
    main()