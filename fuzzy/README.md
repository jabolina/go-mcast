# Artifact tests

This section describes the tests that are available and packed with the artifact to verify the correctness. Some tests 
verified some expected behavior at a high level, and some tests verify the behavior against the formal definition. Tests
that verify the formal definition would be the most secure way to ensure that the artifact is following all the protocol 
requirements.

The complete test suite is executed using always the same configuration, using 3 partitions and for each partition 
exists 3 processes. For each test executed a new instance for the partitions and processes is created, so the test *A* 
does not share processes with test *B*. Were developed tests to verify both the protocol properties and to verify 
specific behaviors.

The protocol properties tests can be found on the `temporal_test.go` file, while the other tests files are tests for
specific behaviors.

---
## Protocol Properties

Some tests were developed to verify that implementation also holds true for the protocol properties. Here the temporal 
factor is the real time, where an event is sent and at the end of the test execution is verified if the output is 
expected. Starting from the first property verified is the *validity* property, which states that:

- If a correct process GM-Cast a message *m* to *m.dest*, then some process in *m.dest* eventually GM-Deliver the 
  message *m*.

To verify this property, the already defined structure of processes is started, and a random generated messages is 
GM-Cast, at the end is verified that the generated message was delivered, is also ensured that *only* the message that 
was GM-Cast was delivered.

The next property is *Agreement*, which was verified along with *Integrity*. Each property, respectively, states that:

- If a correct process GM-Deliver a message *m*, then all correct processes in *m.dest* eventually GM-Deliver the 
  message *m;*
- For any message *m*, every correct process in *m.dest* GM-Deliver *m* at most once, and only if *m* was GM-Cast 
  previously by some process.

In other words, this means that when someone in the destination delivers the message, everyone also should deliver, 
and everyone that delivers the message should do it at most once, and this can only happen if this message was sent 
previously, otherwise the processes could deliver any arbitrary message. To verify both properties a single test was 
created. First is created the structure using 3 partitions and 3 processes per partition, then a message *m* is sent 
to all partitions created and at the end is verified if the final state is expected. On this scenario, to both 
properties to be fulfilled, all partitions will have the same message history log, and the delivered message is the 
message that was sent previously at the start of the test. If, for any reason, the message log history is not equal for 
all partitions, or the message present in the log is an arbitrary message, then the requirements is not met, and the 
test should fail.

At last, the final property verified is the *partial order*, that verifies that even when using the generic portion of 
the protocol to deliver commuting messages and delivering messages to specific partitions, all messages are delivered in
the correct order when necessary. The property definition is:

- If a correct process *p* and *q*, both GM-Deliver messages *m* and *n*, $\{p, q\} \in m.dest \cap n.dest$ and *m* 
  conflict with *n*, then *p* GM-Deliver *m* before *n* if, and only if *q* GM-Deliver *m* before *n.*

To verify this property the same structure is also used here, using 3 partitions named *A, B* and *C*, and 3 processes 
within each partition, for this test the partition is referred by name, so the test can be explained in more details. 
The partitions are split into groups *AB, BC* and  *AC,* so when a message is sent to group *AB* means that a message is 
GM-Cast to partitions *A* and *B,* when a message is broadcasted it is sent to all partitions.

When the test start, multiple messages are sent to each group *AB, BC,* *AC* and **broadcasted. Since the order of 
delivery will only matter if both processes that is being verified are in the message destination, this means that when 
verifying the partition *A* and *B* must have the same history log messages sent to *AB* and broadcasted. So, if a 
process *p* in *A* and process *q* in *B* both delivered a message *m* sent to *AB* and message *n* that was broadcasted, 
*p* can only deliver *m* before *n*, if, and only *q* delivered *m* before *n*.

After sending the messages, is verified that the history log matches for each group *AB*, *BC* and *AC,* so the history 
log of *A* will be verified against *B*, *B* against *C* and *A* against *C.* If during this process the history log 
differs means that messages were delivered in different order between partitions, breaking the *partial order* property.

---
## Atomic Multicast

The already existent structure of groups are used in this tests. The verification here is that the atomic multicast 
property does hold, this means that messages can be sent to only specific partitions and will be delivered in the same 
order for the destination partitions. The strategy here is to send messages to sub-sets of the available partitions, 
with partitions overlapping this sub-sets, at the end verify for each sub-set contains the same messages in the same 
order.

The used structure is the same as for other tests, where exists 3 partitions, inside each partition there are 3 
processes. To verify the multicast, the created sub-sets where *AB*, *AC* and *BC*, and at the end verify that the 
partitions *A* and *B* contains the same messages in the same order where the destination was *AB*, applying the same 
for *AC* and *BC*. This structure is not enough to verify the *partial order* property, since the there are no messages 
*m* and *m'* and processes *p* and *q* that $\{p, q\} \in m.d \cap m'.d$ that spans across partitions, so is only left 
to verify the message total order for the sub-sets.

On this test, was verified that the artifact maintain the order of the messages when sending multicast messages to only 
sub-sets of the available partitions, but this test does not cover the *partial order* property, so a more elaborate 
test could be created to ensure all properties holds. Is also verified that the messages on the log history is the same 
as the messages that were sent, to verify that no arbitrary message was delivered.

---
## Generic

The generic tests verify that even while delivering generic messages on the algorithm, messages that do not commute are 
still delivered in the same order across partitions. To verify this, messages are sent to the protocol to be delivered 
to all partitions, but half of the messages can be delivered at any order, and at the end is verified that all 
partitions contains the messages that do not commute in the same order.

This test verifies the specific generic behavior on the protocol and ensure that this behavior does not harm the total 
order property. Delivering both kinds of messages is also verified on the *partial order* test, that assure that 
conflicting messages are delivered in the same order across partitions.