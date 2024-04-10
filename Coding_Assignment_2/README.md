# 5546-0002 Coding Assignment 2: FIFO-Total Ordered Multicast

This is Coding Assignment 2 for Group 4 in 5545 Distributed Computing Section 2.
This uses the provided skeleton code, modifies it for FIFO/Total-Ordered Multicast that does not use a designated sequencer.  This code is halfway between proper totally-ordered multicast without a sequence and reliable multicast in that it functions with ACKs.  It assumes that sequence number ties are possible and each process uses their own function to sort which order to deliver, like in interactive consistency consensus.

## Execution:

* fifo-total.py: python3 fifo-total.py
    * This executes the multicast with the original list of events.
* fifo-total_extended.py: python3 fifo-total_extended.py
    * This executes the multicast with an extended list of events.

## Requirements:

* The providied simulator.py file must be in the same directory as the fifo* files when executing.  It has been modified from the provided skeleton code.
* This is intended for synchronous systems. If message time is not bounded, it's possible for a message to be never be delivered, in the event that no order is received from the sender before all of the acknowledgements are received from all other processes for messages with the same sequence number.

## Contributors:

Reed White, Penghui Zhu, Lavy Ngo

## Github:

https://github.com/mist861/5546-Distributed-Systems/tree/main/Coding_Assignment_2