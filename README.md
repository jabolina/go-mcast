# go-mcast
![Go](https://github.com/jabolina/go-mcast/workflows/Go/badge.svg?branch=master)

Golang based implementation of the Atomic Multicast protocol


# Introduction

To build fault-tolerant application, one of the possible approach is the use of state machine replication, where 
an application is designed like a state machine and replicated to be executed across *N* replicas. Any system that can 
be structured in terms of procedures and calls, can be structured using state machines and clients. 

To create a fault tolerant application, is now possible to spreading the state machines across different processors. 
All replicas are executed on a non-faulty processor starts at the state and all replicas must receive the same sequence 
of commands in the same order. To guarantee this is needed a replication coordination, that contains two requirements:

- **Agreement:** *every* non faulty component receives *every* request
- **Order:** *every* non faulty component receives *every* request in the same relative order

These requirements can be guaranteed when using an Atomic Broadcast implementation, a communication primitive 
ensures that all *atomic broadcast* message will be delivered to all processes in the same order. Some well-known 
implementations of Atomic Broadcast algorithms are *Raft* and *Paxos.* Using this protocol the requests will be 
ordered across all system.

This approach will guarantee that the system is fault-tolerant, but cannot scale well. To tackle this problem, services 
shards their states thus only needing to have a partial ordering of requests. Reliably delivering requests in partial
order can be solved using an Atomic Multicast implementation.

### Why atomic multicast?

When architecting an application is all about trade-offs, facing the decision about *consistency vs. scalability*, and some
applications can choose between one or another. For an application that prioritize scalability, can be used an eventual
consistency protocol, that given a sharded data is consistent within partition but eventually consistent across partitions
that this data is sharded. This approach can lead to concurrent requests using stale information. 

Some applications have as requirements that the data must be always consistent, even across partitions. Most used protocols
totally order all requests, since the protocol enforces that **all** replicas must receive all requests regardless of the
replica content.

Using atomic broadcast, each request can be sent only to the replicas that are somehow involved in the request process.
In a place where the information is sharded across multiple replicas, this will be more efficient than ordering *every* 
request for *every* replica (why to shard if everyone is receiving requests?). Even though only some replicas are being 
involved in each request, all requests become ordered within and across partitions.


# Atomic Multicast

An atomic multicast protocol, that can be executed on failure-free environment is the Skeen Algorithm, that work as:

```python
def to_multicast(message):
	self.sending[message.id] = message
	for destination in dst(message):
		emit('message', (self.address, message), destination)
	self.on('timestamp', collect_timestamp)

def receive(src, message):
	self.pending.push(message)
	emit('timestamp', (message.id, self.timestamp))

def collect_timestamp(message_id, timestamp):
	if len(dst(self.sending[message_id])) == len(self.timestamps[message_id]):
		message = self.sending[message_id]
		message.sequence_number = max(self.timestamps[message_id])
		for destination in dst(message):
			emit('deliver', message, destination)

def on_deliver(message):
	self.delivery.push(message)

def to_deliver(message):
	if self.delivery.has(lambda m: m != message) \
		and not self.delivery.has(lambda m: m.sequence_number < message.sequence_number):
		deliver(message)
```

Using the Skeen Algorithm all requests will be partially ordered and delivered in a failure-free environment, 
also satisfying the minimality property, thus turning this protocol in a genuine atomic multicast protocol. To this 
algorithm tolerate failure is needed a perfect failure detector on at least a failure detector that can be wrong of on 
a single process. It is well-known in a system with two or more processes in which one process can crash is impossible 
to solve the atomic multicast using a genuine implementation without a perfect failure detector. From this impossibility 
exists a corollary stating that atomic multicast is strictly harder than atomic broadcast.

### Circumventing the impossibility

Still using the Skeen Algorithm as base, but instead of sending TO-multicast messages to individual processes now 
TO-multicast messages to disjoint sets of processes. In this context each set *S* of processes act like a unity and 
every group *g* a majority o members in *g* are correct. Read this as, messages are TO-multicast to groups, in 
this groups some members can fail, but not the majority of members. Every member is only on a single group, thus the 
sets of processes are disjoints.

To choose a sequence number, exists two possible solutions. The first is that each group in *dst(m)* execute a 
consensus instance and agree on the group timestamp, this will cause that for every message occurs |*dst(m)*| consensus 
instances. The second option is that every individual process vote for a timestamp and only a single consensus instance 
for all individual process for group in *dst(m)*. Using this approach, the atomic multicast algorithm is fault 
tolerant and do not require a perfect failure detector.

# Project

The current project aims to implement an optimized version of the atomic multicast algorithm, a version called 
generic multicast algorithm. On the default atomic multicast algorithm exists a *total order* algorithm, this means 
that all requests are ordered. One of the possible optimizations that can occur is to only order requests that needs 
to be ordered and just "go through" with requests that do not need ordering, for example, multiple reads requests 
without actually changing the value do not need to be ordered, so some steps can be skipped. The actual protocol 
implemented is a proposal from Douglas Antunes from Universidade Federal de Uberlandia, this version has some other 
optimizations that are better described in his thesis.

The focus will be to integrate this project with [atomix](https://github.com/atomix/go-client), using the already
available primitives using this project as backend to guarantee consistency.
