# 5546-0002 Coding Assignment 1: Distributed master-worker system

This is Coding Assignment 1 for Group 4 in 5545 Distributed Computing Section 2.
This uses the provided skeleton code, modifies it for round-robin load-balances and worker registration/deregistration.

## Execution:

* Master: python3 master_group4.py {master_port}
* Worker: python3 worker_group4.py {worker_name} {worker_host}:{worker_port} {master_host}:{master:port} {file_group}
* Client (valid): python3 client.py {master_port}
* Client (invalid): python3 client_junk.py {master_port}

## Requirements:

* Client calls must be ran on the same host as the master process, as they were not modified to ensure assignment validity and so do not account for the possibility of the master running on a different host.
* Master and worker processes can be run on different hosts, but connectivity must be in place.
* A file with the name of "data-complete-{file_group}.json" is required in the operating directory of each worker.  The files data-complete-am.json and data-complete-nz.json are provided as examples.

## Contributors:

Reed White, Penghui Zhu, Lavy Ngo