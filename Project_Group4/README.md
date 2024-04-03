# 5546-0002 Team Project: Ensuring Reliable MQ with Unreliable Consumers/Producers

This is the Team Project for Group 4 in 5545 Distributed Computing Section 2.

## Execution:

* Producer: python3 producer.py {producer_port}
* Consumer: python3 consumer.py
* Client (valid): python3 client.py {producer_host} {producer_port} {iteration_number} {run_id}
* Producer (control): python3 producer_control.py {producer_port}
* Consumer (control): python3 consumer_control.py

## Requirements:

* Producer and Consumer both require an accessible MySQL database instance with the appropriate users/privileges and the following schema/tables:
    * Producer: distributed_producer (schema), Sent (table), Failed (table)
    * Consumer: distributed_consumer (schema), Received (table)
* Producer and Consumer both require an accessible RabbitMQ instance with the appropriate users/privileges and the following queues:
    * distributed_test_service, distributed_test_ack
* Producer requires an accessible Redis cache with appropriate users/privileges

## Contributors:

Reed White, Penghui Zhu, Lavy Ngo

## Github:

https://github.com/mist861/5546-Distributed-Systems/tree/main/Project_Group4
