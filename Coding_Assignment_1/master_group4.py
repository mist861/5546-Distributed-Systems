from curses.ascii import isdigit
from decimal import localcontext
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import sys
import re #for performing regex matches, used to select nodes based on name
import html #for sanitizing input


workers = {} #global master dict for keeping track of workers
group_am = [] #global array for keeping track of which workers are in am
group_nz = [] #global array for keeping track of which workers are in nz

def registerworker(worker_name, worker_host, port):
    global workers, group_am, group_nz #load the global vars
    if len(group_am) == len(group_nz): #if the two arrays have the same number of members, default to the am group
        workers.update({f'{worker_name}': ServerProxy(f'http://{worker_host}:{port}')}) #append/update the worker to the master dict
        group_am.append(worker_name) #append the worker to the am group
        print(f'{worker_name} registered in group am!') #info statement
        print(workers) #info statement
        return 'am' #return the group name to the worker, so that it knows which file to use
    elif len(group_am) > len(group_nz): #if the am group has more members, add the new worker to the nz group
        workers.update({f'{worker_name}': ServerProxy(f'http://{worker_host}:{port}')}) #append/update the worker to the master dict
        group_nz.append(worker_name) #append the worker to the nz group
        print(f'{worker_name} registered in group nz!') #info statement
        print(workers) #info statement
        return 'nz' #return group name to the worker, so that it knows which file to use
    elif len(group_am) < len(group_nz): #reverse of the above, with the same steps
        workers.update({f'{worker_name}': ServerProxy(f'http://{worker_host}:{port}')})
        group_am.append(worker_name)
        print(f'{worker_name} registered in group am!')
        print(workers)
        return 'am'
      
def getbylocation(location):
    location = html.escape(location) #sanitize the user input
    print(f'Request for getbylocation({location}) received.') #info statement after sanitizing
    result = {} #init empty dict to temporarily store results
    result_list = {} #init empty dict to store combined results
    error = False #init bool to track if there were any errors, default to False becasue no errors are expected
    try:
        result.update(workers['worker-1'].getbylocation(location)) #get results (if any) from rpc call to worker-1, stored in result
        if result.get('error') == False: #if the rpc call succeeds and doesn't return an error (since result is a dict with a key "error" with possible values "True" and False"), do:
            print('Results found in worker-1') #info statement
            result_list.update(result.get('result')) #append the result (which is in the form of a dict) to the result_list dict
    except:
            print('There was an issue calling worker-1!')
            error = True
    try:
        result.update(workers['worker-2'].getbylocation(location)) #get results (if any) from rpc call to worker-2, stored in result
        if result.get('error') == False: #if the rpc call succeeds and doesn't return an error, do:
            print('Results found in worker-2') #info statement
            result_list.update(result.get('result')) #append the result to the result_list dict
    except:
            print('There was an issue calling worker-2!')
            error = True
    if not result_list and error == False: #if the result_list dict is still empty, then:
        return {
            'error': error,
            'result': 'No users were found for that location.'
        }
    elif not result_list and error == True:
        return {
            'error': error,
            'result': 'There was an error in the system.'
        }
    elif error == True:
        return {
            'error': error,
            'warning': 'There was an error in part of the system, results may not be complete',
            'result': result_list
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
    if re.findall(r'\A[a-mA-M]', name): #returns true if the first letter of the name input is a-m or A-M
        print('Sending to worker-1') #info statement
        try:
            result.update(workers['worker-1'].getbyname(name)) #get results (if any) from rpc call to worker-1, stored in result
            if result.get('error') == False: #if the rpc call succeeds and doesn't return an error (since result is a dict with a key "error" with possible values "True" and False"), do:
                result_list.update(result.get('result')) #append the result (which is in the form of a dict) to the result_list dict
        except:
            print('There was an issue calling worker-1!')
            error = True
    elif re.findall(r'\A[n-zN-Z]', name): #returns true if the first letter of the name input is n-z or N-Z
        print('Sending to worker-2') #info statement
        try:
            result.update(workers['worker-2'].getbyname(name)) #get results (if any) from rpc call to worker-2, stored in result
            if result.get('error') == False: #if the rpc call succeeds and doesn't return an error, do:
                result_list.update(result.get('result')) #append the result (which is in the form of a dict) to the result_list dict
        except:
            print('There was an issue calling worker-2!')
            error = True
    if not result_list and error == False: #if the result_list dict is still empty, then:
        return {
            'error': error,
            'result': 'No user found with that name'
        }
    elif  error == True:
        return {
            'error': error,
            'result': 'There was an error in the system.'
        }
    else:
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
        try:    
            result.update(workers['worker-1'].getbyyear(location, year)) #get results (if any) from rpc call to worker-1, stored in result
            if result.get('error') == False: #if the rpc call succeeds and doesn't return an error (since result is a dict with a key "error" with possible values "True" and False"), do:
                print('Results found in worker-1') #info statement
                result_list.update(result.get('result')) #append the result (which is in the form of a dict) to the result_list dict
        except:
            print('There was an issue calling worker-1!')
            error = True 
        try:
            result.update(workers['worker-2'].getbyyear(location, year)) #get results (if any) from rpc call to worker-2, stored in result
            if result.get('error') == False: #if the rpc call succeeds and doesn't return an error, do:
                print('Results found in worker-2') #info statement
                result_list.update(result.get('result')) #append the result (which is in the form of a dict) to the result_list dict
        except:
            print('There was an issue calling worker-1!')
            error = True 
    if not result_list and error == False: #if the result_list dict is still empty, then:
        return {
            'error': error,
            'result': 'No users were found for that location.'
        }
    elif not result_list and error == True:
        return {
            'error': error,
            'result': 'There was an error in the system.'
        }
    elif error == True:
        return {
            'error': error,
            'warning': 'There was an error in part of the system, results may not be complete',
            'result': result_list
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