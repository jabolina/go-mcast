--------------------    MODULE mcast0    --------------------

EXTENDS Naturals, FiniteSets

CONSTANTS 
    \* `NPROCESS` is the number of nodes that the system will have.
    \* Must be a Natural number, greater than 0.
    NPROCESS,

    \* Is the number of messages the system will exchange.
    \* Must be a Natural number greater than 0.
    NMESSAGES,

    \* The conflict relationship to verify if messages can commute.
    \* This function will receive two messages and verify if they
    \* can commute or not, this must be deterministic across time.
    \* If two messages `m` and `n` are verified in a moment `t` and
    \* in an moment `t'`, then in both period `t` and `t'` the response
    \* is the same, the behaviour can not change across time.
    Conflict(_, _)

\* Set containing the identifier for every nodes inside the system.
Processes == {i : i \in 1 .. NPROCESS}

\* Set containing all possible message identifiers.
\* The message identifiers must be defined previously.
Messages == 1 .. NMESSAGES

\* Choose a random process available on the system.
ChooseProcess == CHOOSE x \in Processes : TRUE

\* Conflict relationship used in this model.
\* This is just a dummy conflict to verify that the model holds true
\* for all properties.
\* This conflict relationship verify if the given messages both contains
\* an identifier that is even or odd, if both arguments are equals the
\* messages do not commute.
IsEven(x) == x % 2 = 0
ByIdConflict(x, y) == IsEven(x.id) = IsEven(y.id)
AlwaysConflict(x, y) == TRUE

ChooseSubset == CHOOSE x \in SUBSET Processes: Cardinality(x) > 0

\* Contains all messages the system will exchange.
ToSend == { [ id |-> id, d |-> Processes, ts |-> 0, s |-> ChooseProcess ] : id \in Messages }

\* This set will hold information about GMCast calls that were made. 
\* But since only a single process actually call GMCast, this set will 
\* hold the tuple <<node, msg>>, thus only a single process GMCast(msg).
GMCast == { <<m.s, m>> : m \in ToSend }

\* Create a new message from a existent one, updating the timestamp
\* with the given value.
CreateMessage(m, k) == [ id |-> m.id, d |-> m.d, ts |-> k ]

\* Select the greater value inside a set.
Max(S) == CHOOSE x \in S: \A y \in S : x >= y

\* Verify the input values.
ASSUME 
    \* Verify that `NPROCESS` is a natural number greater than 0.
    /\ NPROCESS \in (Nat \ {0})

    \* Verify that the number of messages to exchange is natural
    \* greater than 0.
    /\ NMESSAGES \in (Nat \ {0})

(***************************************************************************)
(*                                                                         *)
(*     All variables are global here and basically every variable is a     *)
(* structure indexed by the node identifier. For each index, we follow the *)
(* protocol definition, using a set for holding the values for each        *)
(* process. We also follow the same nomenclature used on the algorithm for *)
(* the variables.                                                          *)
(*                                                                         *)
(*    Here we define all the protocol structures as well some helpers.     *)
(*                                                                         *)
(***************************************************************************)
VARIABLES
    \* Structure that holds the clocks for all processes.
    K,

    \* Structure that holds all messages that were received
    \* but are still pending a final timestamp.
    Pending,

    \* Structure that holds all messages that contains a
    \* final timestamp but were not delivered yet.
    Delivering,

    \* Structure that holds all messages that contains a
    \* final timestamp and were already delivered.
    Delivered,

    \* Used to verify if previous messages conflict with
    \* the message beign processed. Using this approach
    \* is possible to deliver messages with a partially
    \* ordered delivery.
    PreviousMsgs,

    (***************************************************************************)
    (*                                                                         *)
    (* Bellow here we start defining some helper structures that are not       *)
    (* defined nor needed by the protocol itself.                              *)
    (*                                                                         *)
    (***************************************************************************)

    \* Structure to simulate a network call.
    \* Messages are exchanged using the format <<State, Msg>>, so a single
    \* structure can be used across different states the protocol exchange
    \* messages between processes.
    Network,

    \* Set used to holds the votes that were cast for a message.
    \* Since the coordinator needs that all processes cast a vote for the
    \* final timestamp, this structure will hold the votes each process
    \* cast for each message on the system.
    Votes

\* State holding all variables used on the spec.
vars == <<
    Network,
    Votes,
    K, 
    Pending,
    Delivering,
    Delivered,
    PreviousMsgs >>

(***************************************************************************)
(*                                                                         *)
(*     Responsible for initializing gloabal variables used on the system.  *)
(* All variables that are defined by the protocol is a mapping from the    *)
(* node id to the corresponding process set.                               *)
(*                                                                         *)
(*     The `message` is also a structure, with the following format:       *)
(*                                                                         *)
(*              [ id |-> Nat, ts |-> Nat, d |-> Nodes ]                    *)
(*                                                                         *)
(* The `d` does not need to be the whole Nodes, only one of the possible   *)
(* SUBSET Nodes. The keys representation is `id` the unique message id,    *)
(* `ts` is the message timestamp/sequence number, `d` is the destination.  *)
(* In some steps, a property may be added, for example the `s` property    *)
(* that holds the initial source of the message or the property `o` that   *)
(* is used when casting votes and is used to identiy the process id,       *)
(*                                                                         *)
(***************************************************************************)
InitProtocol ==
    \* Start all nodes with clock set to 0.
    /\ K = [ i \in Processes |-> 0 ]
    
    \* Start all nodes with the messages set empty.
    /\ Pending = [ i \in Processes |-> {} ]
    /\ Delivering = [ i \in Processes |-> {} ]
    /\ Delivered = [ i \in Processes |-> {} ]
    /\ PreviousMsgs = [ i \in Processes |-> {} ]

\* Here we start all helpers.
InitHelpers ==
    \* This is used to simulate network calls. The network is started
    \* with messages on state "S0" that are ready to be processed.
    /\ Network = [ i \in Processes |-> {<<"S0", m>> : m \in {x \in ToSend: i \in x.d}} ]

    \* This structure is holding the votes the processes cast for each
    \* message on the system. Since any process can be the "coordinator",
    \* this is a mapping for processes to a set. The set will contain the
    \* vote a process has cast for a message.
    /\ Votes = [ i \in Processes |-> {} ]

Init == InitProtocol /\ InitHelpers
    
(***************************************************************************)
(*                                                                         *)
(*     Bellow here is defined the protocol. Each step uses the same name   *)
(* used on the protocol definition.                                        *)
(*                                                                         *)
(***************************************************************************)
-----------------------------------------------------------------------------

(***************************************************************************)
(*                                                                         *)
(*     After a process sends GM-Cast the message `m`, every process in m.d *)
(* receives the message, assing the locale clock to the message timestamp, *)
(* insert the message with timestamp to the process `Pending` set and      *)
(* sends it to the coordinator choose the timestamp.                       *)
(*                                                                         *)
(*     To compute the timestamp, the process will verify if the current    *)
(* message conflict with previous messages, and only increase the local    *)
(* clock if the message conflict with previous messages.                   *)
(*                                                                         *)
(*     Is not defined on the protocol, but here we are also adding the     *)
(* node vote for the timestamp in the structure that holds the votes.      *)
(*                                                                         *)
(***************************************************************************)
AssignTimestamp(self) ==
    \E <<state, msg>> \in Network[self]: 
        /\ state = "S0"
        /\ \/ /\ \E prev \in PreviousMsgs[self]: Conflict(msg, prev)
              /\ K' = [K EXCEPT ![self] = K[self] + 1]
              /\ PreviousMsgs' = [PreviousMsgs EXCEPT ![self] = {msg}]
           \/ /\ \A prev \in PreviousMsgs[self]: ~Conflict(msg, prev)
              /\ K' = [K EXCEPT ![self] = K[self]]
              /\ PreviousMsgs' = [PreviousMsgs EXCEPT ![self] = PreviousMsgs[self] \cup {msg}]
        /\ LET
            built == [ id |-> msg.id, d |-> msg.d, ts |-> K'[self] ]
            voted == [ id |-> msg.id, d |-> msg.d, s |-> msg.s, ts |-> K'[self], o |-> self ]
            IN
            /\ Pending' = [Pending EXCEPT ![self] = Pending[self] \cup {built}]
            /\ Network' = [dest \in Processes |-> IF dest \in msg.d THEN
                                IF self = dest /\ msg.s = self
                                THEN (Network[self] \ {<<state, msg>>}) \cup {<<"S1", voted>>}
                                ELSE IF msg.s = dest
                                    THEN Network[dest] \cup {<<"S1", voted>>}
                                    ELSE IF self = dest
                                        THEN  Network[dest] \ {<<state, msg>>}
                                        ELSE Network[dest]
                            ELSE IF msg.s = dest
                                THEN Network[dest] \cup {<<"S1", voted>>}
                                ELSE Network[dest] \ {<<state, msg>>}]
            /\ UNCHANGED <<Delivering, Delivered, Votes>>

(***************************************************************************)
(*                                                                         *)
(*     This method is executed only by the "coordinator", this coordinator *)
(* is the original source of the message. This method processes messages   *)
(* on state S1, and can proceed in two ways. The first option is that we   *)
(* already have all votes needed for the message, so we can proceed to     *)
(* choose the final timestamp and broadcast the message on state S2 to all *)
(* processes. If the messages does not have all needed votes, then we save *)
(* the vote for the message on the `Votes` structure.                      *)
(*                                                                         *)
(***************************************************************************)
ComputeSeqNumber(self) ==
    \E <<state, msg>> \in {<<s, x>> \in Network[self]: s = "S1" /\ x.s = self}:
        LET
            votedTs == {<<m.o, m.ts>> : m \in {x \in Votes[self] \cup {msg}: x.id = msg.id}}
        IN
            /\ \/ /\ Cardinality(votedTs) = Cardinality(msg.d)
                  /\ LET
                      built == CreateMessage(msg, Max({x[2] : x \in votedTs}))
                     IN
                      /\ Votes' = [Votes EXCEPT ![self] = {x \in Votes[self] : x.id /= msg.id}]
                      /\ Network' = [dest \in Processes |-> IF dest \in msg.d
                                        THEN (Network[dest] \ {<<state, msg>>}) \cup {<<"S2", built>>}
                                        ELSE Network[dest]]
                      /\ UNCHANGED <<K, PreviousMsgs, Pending, Delivering, Delivered>>
               \/ /\ Cardinality(votedTs) < Cardinality(msg.d)
                  /\ Votes' = [Votes EXCEPT ![self] = Votes[self] \cup {msg}]
                  /\ Network' = [Network EXCEPT ![self] = @ \ {<<state, msg>>}]
                  /\ UNCHANGED <<K, PreviousMsgs, Pending, Delivering, Delivered>>

(***************************************************************************)
(*                                                                         *)
(*    After the coordinator compute the final timestamp for the message    *)
(* `m` all processes in `m.d` will receive the choosen timestamp. So for   *)
(* each process the final timestamp will be verified against its own local *)
(* clock, if the agreed value is greater the process clock will leap. If   *)
(* the received message conflict with a previos message, then we will also *)
(* increase the clock. If the message does not conflict, then we only leap *)
(*                                                                         *)
(*    After assigning the final timestamp, the message will be added to    *)
(* the `Delivering` to be delivered when ready and will be removed the     *)
(* buffer `Pending`, since the message already have a timestamp.           *)
(*                                                                         *)
(***************************************************************************)
AssignSeqNumber(self) ==
    /\ \E m1 \in Pending[self] :
        \E <<state, m2>> \in Network[self] :
            /\ state = "S2" 
            /\ m2.id = m1.id
            /\ \/ /\ m2.ts > K[self]
                  /\ \/ /\ \E prev \in PreviousMsgs[self]: Conflict(m2, prev)
                        /\ K' = [K EXCEPT ![self] = m2.ts + 1]
                        /\ PreviousMsgs' = [PreviousMsgs EXCEPT ![self] = {}]
                     \/ /\ \A prev \in PreviousMsgs[self]: ~Conflict(m2, prev)
                        /\ K' = [K EXCEPT ![self] = m2.ts]
                        /\ UNCHANGED PreviousMsgs
               \/ /\ m2.ts <= K[self]
                  /\ UNCHANGED <<K, PreviousMsgs>>
            /\ Pending' = [Pending EXCEPT ![self] = @ \ {m1}]
            /\ Delivering' = [Delivering EXCEPT ![self] = Delivering[self] \cup {m2}]
            /\ Network' = [Network EXCEPT ![self] = @ \ {<<state, m2>>}]
            /\ UNCHANGED <<Votes, Delivered>>

(***************************************************************************)
(*                                                                         *)
(*    Responsible for delivering messages that can be delivered, messages  *)
(* that are ready to be delivered are present on the `Delivering` set and  *)
(* contains the smallest timestamp between all other messages `Pending`    *)
(* and `Delivering` sets. We can also deliver messages that can commute,   *)
(* this is the generalized behaviour in action.                            *)
(*                                                                         *)
(*    Delivered messages will be added to the `Delivered` set and will be  *)
(* removed from the others sets. To verify the instant the messages were   *)
(* delivered, the messages will be added with the following format:        *)
(*                                                                         *)
(*                     <<Len(Delivered), {messages}>>                      *)
(*                                                                         *)
(*    Using this model, we know the order messages were delivered for each *)
(* process.                                                                *)
(*                                                                         *)
(***************************************************************************)
DoDeliver(self) ==
    \E m \in Delivering[self]:
        /\ \A n \in (Delivering[self] \cup Pending[self]) \ {m}:
            /\ m.ts <= n.ts
            /\ \/ ~Conflict(m, n)
               \/ m.id < n.id \/ m.ts < n.ts
        /\ LET
            T == Delivering[self] \cup Pending[self]
            G == {x \in Delivering[self]: \A y \in T \ {x}: ~Conflict(x, y)}
            F == {m} \cup G
            index == Cardinality(Delivered[self])
           IN
            /\ Delivering' = [Delivering EXCEPT ![self] = @ \ F]
            /\ Delivered' = [Delivered EXCEPT ![self] = Delivered[self] \cup {<<index, F>>}]
            /\ UNCHANGED <<Network, Votes, Pending, PreviousMsgs, K>>

-----------------------------------------------------------------------------
Step(self) ==
    \/ AssignTimestamp(self)
    \/ ComputeSeqNumber(self)
    \/ AssignSeqNumber(self)
    \/ DoDeliver(self)

Next == 
    \/ \E self \in Processes: Step(self)
    \/ UNCHANGED vars

Spec == Init /\ [][Next]_vars
             /\ WF_vars(\E self \in Processes: Step(self))

-----------------------------------------------------------------------------
\* Filter the messages on the `Delivered` set, to find the tuple that holds
\* the information for the given message.
FilterDeliveredMessage(p, m) ==
    { <<idx, msgs>> \in Delivered[p] : \E n \in msgs : n.id = m.id }

\* Find the instant a message were delivered in a specific process.
DeliveredIndex(p, m) ==
    (CHOOSE <<index, msgs>> \in Delivered[p]: \E n \in msgs: m.id = n.id)[1]

\* Verify if the given process delivered the given message.
WasDelivered(p, m) ==
    /\ \E <<idx, msgs>> \in Delivered[p] :
        \E n \in msgs : 
            n.id = m.id

(***************************************************************************)
(*                                                                         *)
(*     If a correct process GM-Cast a message `m` to `m.d`, then some      *)
(* process in `m.d` eventually GM-Deliver `m`.                             *)
(*                                                                         *)
(*     We verify that all messages on the messages that will be send, then *)
(* we verify that exists a process on the existent processes that did sent *)
(* the message and eventually exists a process on `m.d` that delivers the  *)
(* message.                                                                *)
(*                                                                         *)
(***************************************************************************)
Validity ==
    \A m \in ToSend:
        \E p \in Processes:
            <<p, m>> \in GMCast ~> \E q \in m.d : WasDelivered(q, m)

(***************************************************************************)
(*                                                                         *)
(*     If a correct process GM-Deliver a message `m`, then all correct     *)
(* processes in `m.d` eventually GM-Deliver `m`.                           *)
(*                                                                         *)
(*     We verify that all messages on the messages that will be send, then *)
(* we verify that exists a process and it did deliverd the message so we   *)
(* verify that eventually all processes in `m.d` also delivers `m`.        *)
(*                                                                         *)
(***************************************************************************)
Agreement ==
    \A m \in ToSend:
        \A p \in Processes:
            WasDelivered(p, m) ~> \A q \in m.d : WasDelivered(q, m)

(***************************************************************************)
(*                                                                         *)
(*     For any message `m`, every correct process in `m.d` GM-Deliver `m`  *)
(* at most once, and only if `m` was previously GM-Cast by some process.   *)
(*                                                                         *)
(*     We verify that all messages on the messages that will be send, all  *)
(* processes on `m.d` that delivered the message and delivered it only     *)
(* once, this implies that exists a process that GMCast the message        *)
(* previously.                                                             *)
(*                                                                         *)
(***************************************************************************)
DeliveredOnlyOnce(p, m) == Cardinality(FilterDeliveredMessage(p, m)) = 1
Integrity == 
    \A <<p, m>> \in GMCast:
        p \in Processes ~> \A q \in m.d : (WasDelivered(q, m) /\ DeliveredOnlyOnce(q, m))

(***************************************************************************)
(*                                                                         *)
(*     If a correct processes `p` and `q` both GM-Deliver message `m` and  *)
(* `m'` (p, q \subset (m.d \inter m'.d)) and m conflicts with m', then `p` *)
(* GM-Deliver `m` before `m'` iff `q` GM-Deliver `m` before `m'`.          *)
(*                                                                         *)
(***************************************************************************)
AssertDeliveryOrder(pm, pn, qm, qn) == 
    \/ ((pm < pn) /\ (qm < qn))
    \/ (~(pm < pn) /\ ~(qm < qn))
BothDelivered(p, q, m, n) ==
    /\ WasDelivered(p, m) /\ WasDelivered(p, n)
    /\ WasDelivered(q, m) /\ WasDelivered(q, n)
LHS(p, q, m, n) ==
    /\ {p, q} \subseteq (m.d \intersect n.d)
    /\ Conflict(m, n)
    /\ BothDelivered(p, q, m, n)
RHS(p, q, m, n) ==
    /\ LET
        pm == DeliveredIndex(p, m)
        pn == DeliveredIndex(p, n)
        qm == DeliveredIndex(q, m)
        qn == DeliveredIndex(q, n)
        IN
        AssertDeliveryOrder(pm, pn, qm, qn)
PartialOrder ==
    [](\A p, q \in Processes:
        \A m, n \in ToSend:
            LHS(p, q, m, n) => RHS(p, q, m, n))

(***************************************************************************)
(*                                                                         *)
(*     If a correct processes `p` GM-Deliver both messages `m` and `m'`,   *)
(* `m` != `m'`, `p` is in (m.d \inter m'.d)) and m conflicts with m', then *)
(* message `m` was delivered in a different moment than message `m`.       *)
(*                                                                         *)
(***************************************************************************)
Collision ==
    []\A p \in Processes:
        \A m, n \in ToSend:
            /\ m.id /= n.id
                /\ p \in (m.d \intersect n.d)
                /\ WasDelivered(p, m)
                /\ WasDelivered(p, n)
                /\ Conflict(m, n) => DeliveredIndex(p, m) /= DeliveredIndex(p, n)
==============================================================
