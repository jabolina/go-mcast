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

This approach will guarantee that the system is fault-tolerant, but not scale well, since all replicas must receive 
all requests. To tackle this problem, services shards their states thus only needing to have a partial ordering of 
requests. Reliably delivering requests in partial order can be solved using an Atomic Multicast implementation, 
where only a subset of the system nodes receive the needed messages.

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

An atomic multicast protocol, that can be executed on failure-free environment is the Skeen Algorithm, that uses logical 
clocks to give a sequence number for each message before delivering. Having a coordinator that sends the message to each 
destination processes, then gather all timestamp that each process proposed and define a final timestamp. Using the final 
timestamp the messages are sorted and delivered.

A fault-tolerant version of the Skeen's algorithm, proposed by Fritzke, uses group of processes instead of single 
processes on the destination end. When the coordinator sends the message to the destination, is not a single destination 
process, is now a group of processes that works like a unity. Using this approach, even on the face of failures the 
protocol can be considered reliable if exist a quorum of correct processes on the unity destination. On this protocol, 
first each group will agree on the group timestamp, then the groups will exchange the timestamp between them and after 
this the maximum value is used as the final timestamp, and like the Skeen's algorithm, the messages are sorted and 
delivered accordingly with the final timestamp.

Some improvements were proposed by Schiper and Pedone on the Fritzke algorithm, about some practical aspects of the 
atomic multicast algorithm and some impossibilities. It is well-known in a system with two or more processes in which 
one process can crash is impossible to solve the atomic multicast using a genuine implementation without a perfect 
failure detector. From this impossibility exists a corollary that atomic multicast is strictly harder than atomic broadcast.

# Generic Multicast

The atomic multicast, even with the proposed improvements still can be an expensive primitive communication to build 
reliable and fault-tolerant applications, since all messages will be ordered, when a better approach would be to order 
only messages that need to be ordered. Using this method for relaxing the messages delivery order is the Generic Multicast, 
this can be interesting since is possible that only conflicting messages can be ordered and is possible to declare this 
relationship at application level, suiting best what is going to be built. Generic Multicast is defined by the primitives 
*GM-Cast* and *GM-Deliver* following properties:

- *Validity*: If a correct process *GM-Cast* a message *m* to *Dest<m>*, then some process in *Dest<m>* eventually *GM-Deliver m;*
- *Agreement*: If a correct process *GM-Deliver* a message *m,* then all correct processes in *Dest<m>* eventually *GM-Deliver<m>;*
- *Integrity*: For any message *m*, every correct process in *Dest<m>* *GM-Deliver m* at most once, and only if *m* was previously *GM-Cast* by some process;
- *Partial Order*: If correct processes *p* and *q* both *GM-Deliver* messages *m* and *m'* and *p* and *q* belongs to *Dest<m>* united with *Dest<m'>* and *m* and *m'* conflicts, then *p GM-Deliver m* before *m'* iff *q* *GM-Deliver m* before *m'.*

The current project aims to implement a Generic Atomic Multicast that can be used as a primitive to build fault-tolerant 
applications. The algorithm here was proposed by Douglas Antunes and Lasaro Camargos at Universidade Federal de Uberlândia.

### Model and primitives

This work was done considering a set of processes that can fail by crashing or not failing at all, hence processes do 
not behave maliciously. Communication is done using message on transport channels that do not corrupt nor duplicate 
messages and which is quasi-reliable. The system is asynchronous with unreliable failure detectors.

Since communication is done using message passing, some communication primitives are used. The first and more simple one is:

- *Send<m, p>*: send message *m* to process *p;*
- *Recv<m>*: happens at the destination when a new message is received;

Both of the above primitives are unreliable and can be used the existent primitives from the transport layer for 
process communication. This is used to enable unicast process communication, when a message needs to be sent to a 
single process and there is no need for reliability.

Another primitive needed and used for group communication are:

- *GB-Cast<m>* and *GB-Deliver<m>*

This primitive will be used for group communication inside a process unity. This primitive is *Generic Broadcast,* 
that work similar as primitives like *Atomic Broadcast* but prevent the ordering of message when not necessary, taking 
into account the semantics of the messages to establish a partial order on message delivery. This primitive is specified 
by a conflict relationship *C* and the following conditions:

- *Validity*: If a correct process *GB-Cast* a message *m*, then eventually *GB-Deliver* m;
- *Agreement*: If a correct process *GB-Deliver* a message *m*, then all correct processes eventually *GB-Deliver* *m;*
- *Integrity*: For any message *m*, every correct process *GB-Deliver m* at most once, and only if *m* was previously *GB-Cast* by some process;
- *Partial Order*: If the correct processes *p* and *q* both *GB-Deliver* messages *m* and *m'*, and *m* and *m'* conflict, then *p GB-Deliver m* before *m'* iff *q GB-Deliver m* before *m'*;

Using a Total Order Broadcast is possible to solve the Generic Broadcast problem, but is not the ideal solution.

# References

PEDONE, F.; SCHIPER, A. Generic broadcast. In: SPRINGER. International Symposium on Distributed Computing. [S.l.], 1999. p. 94–106.

GUERRAOUI, R.; SCHIPER, A. Genuine atomic multicast in asynchronous distributed systems. 2001. Theoretical Computer Science, 254(1–2), 297–316.

AHMED-NACER, T.; SUTRA, P.; CONAN, D. The convoy effect in atomic multicast. In: IEEE. 2016 IEEE 35th Symposium on 
Reliable Distributed Systems Workshops (SRDSW). [S.l.], 2016. p. 67–72.

ANTUNES, D. A Fault-Tolerant Generic Multicast Algorithm for Wide Area Networks. 2019.

BENZ, S.; MARANDI, P. J.; PEDONE, F.; GARBINATO, B. Building global and scalable systems with atomic multicast. 2014. In Middleware 
Conference 2014 - Proceedings of the Posters and Demos Session (pp. 11–12). Association for Computing Machinery, Inc.
 