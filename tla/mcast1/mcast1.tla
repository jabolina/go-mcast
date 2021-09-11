--------------------    MODULE mcast1    --------------------

EXTENDS Naturals, FiniteSets, Sequences, ABC, Network, Helper

CONSTANTS 
    NPARTITIONS,
    NPROCESS,
    NMESSAGES,
    Conflict(_,_)

\* Verify the input values.
ASSUME 
    /\ NPARTITIONS \in (Nat \ {0})

    \* Verify that `NPROCESS` is a natural number greater than 0.
    /\ NPROCESS \in (Nat \ {0})

    \* Verify that the number of messages to exchange is natural
    \* greater than 0.
    /\ NMESSAGES \in (Nat \ {0})

IsEven(x) == x % 2 = 0
ByIdConflict(x, y) == IsEven(x.id) = IsEven(y.id)
AlwaysConflict(x, y) == TRUE

IsMajority(s) == Cardinality(s) >= (NPROCESS \div 2) + 1

AllProcesses == 1 .. NPROCESS
Processes == CHOOSE v \in (SUBSET AllProcesses): IsMajority(v)
Partitions == 1 .. NPARTITIONS

ChoosePartition == CHOOSE p \in SUBSET Partitions: Cardinality(p) > 0
Messages == [m \in 1 .. NMESSAGES |-> [id |-> m, d |-> Partitions, ts |-> 0, s |-> 0]]
ToSend == {Messages[i] : i \in 1 .. NMESSAGES}

VARIABLES 
    K,
    Mem,
    PreviousMsgs,
    ABCast,
    Delivered,
    ProcessComm

vars == <<
    K,
    Mem,
    PreviousMsgs,
    Delivered,
    ABCast,
    ProcessComm >>

--------------------------------------------------------------
InitProtocol ==
    /\ K = [i \in Partitions |-> [p \in Processes |-> 0]]
    /\ Mem = [i \in Partitions |-> [p \in Processes |-> {}]]
    /\ PreviousMsgs = [i \in Partitions |-> [p \in Processes |-> {}]]
    /\ Delivered = [i \in Partitions |-> [p \in Processes |-> {}]]

InitHelpers ==
    /\ ABCast = [i \in Partitions |-> [p \in Processes |-> Messages]]
    /\ ProcessComm = [i \in Partitions |-> [p \in Processes |-> {}]]

Init == InitProtocol /\ InitHelpers

ComputeGroupSeqNumber(p, q) ==
    /\ HasValue(ABCast[p][q])
    /\ LET
        m == Peek(ABCast[p][q])
       IN
        /\ \/ /\ m.s = 0
              /\ \/ /\ \E n \in PreviousMsgs[p][q]: Conflict(m, n)
                    /\ K' = [K EXCEPT ![p][q] = K[p][q] + 1]
                    /\ PreviousMsgs' = [PreviousMsgs EXCEPT ![p][q] = {m}]
                 \/ /\ \A n \in PreviousMsgs[p][q]: ~Conflict(m, n)
                    /\ PreviousMsgs' = [PreviousMsgs EXCEPT ![p][q] = PreviousMsgs[p][q] \cup {m}]
                    /\ UNCHANGED K
           \/ /\ m.s /= 0
        /\ \/ /\ Cardinality(m.d) > 1
              /\ \/ /\ m.s = 0
                    /\ Mem' = [Mem EXCEPT ![p][q] = InsertOrUpdate(Mem[p][q], [id |-> m.id, d |-> m.d, ts |-> K'[p][q], s |-> 1])]
                    /\ ProcessComm' = Send(ProcessComm, [id |-> m.id, d |-> m.d, ts |-> K'[p][q], s |-> 1, o |-> p])
                 \/ /\ m.s = 2
                    /\ \/ /\ m.ts > K[p][q]
                          /\ K' = [K EXCEPT ![p][q] = m.ts]
                          /\ PreviousMsgs' = [PreviousMsgs EXCEPT ![p][q] = {}]
                       \/ /\ m.ts <= K[p][q]
                          /\ UNCHANGED <<K, PreviousMsgs>>
                    /\ Mem' = [Mem EXCEPT ![p][q] = InsertOrUpdate(Mem[p][q], [id |-> m.id, d |-> m.d, ts |-> m.ts, s |-> 3])]
                    /\ UNCHANGED ProcessComm
           \/ /\ Cardinality(m.d) = 1
              /\ Mem' = [Mem EXCEPT ![p][q] = InsertOrUpdate(Mem[p][q], [id |-> m.id, d |-> m.d, ts |-> K'[p][q], s |-> 3])]
              /\ UNCHANGED ProcessComm
        /\ ABCast' = [ABCast EXCEPT ![p][q] = ABDeliver(ABCast[p][q])]
        /\ UNCHANGED Delivered

GatherGroupsTimestamp(p, q) ==
    \E m \in Mem[p][q]: HasReceivedFromAllPartitions(m, ProcessComm[p][q])
        /\ LET
            timestamps == {v.ts : v \in {x \in ProcessComm[p][q]: x.id = m.id}}
            msgs == {x \in ProcessComm[p][q]: x.id = m.id}
            n == [id |-> m.id, d |-> m.d, ts |-> Max(timestamps), s |-> 2]
           IN
            /\ \/ /\ m.ts >= Max(timestamps)
                  /\ Mem' = [Mem EXCEPT ![p][q] = InsertOrUpdate(Mem[p][q], [id |-> m.id, d |-> m.d, ts |-> m.ts, s |-> 3])]
                  /\ UNCHANGED <<K, PreviousMsgs, ABCast, Delivered>>
               \/ /\ m.ts < Max(timestamps)
                  /\ Mem' = [Mem EXCEPT ![p][q] = InsertOrUpdate(Mem[p][q], n)]
                  /\ ABCast' = [ABCast EXCEPT ![p] = AtomicBroadcast(ABCast[p], n)]
                  /\ UNCHANGED <<K, PreviousMsgs, Delivered>>
            /\ ProcessComm' = [ProcessComm EXCEPT ![p][q] = @ \ msgs]

DoDeliver(p, q) ==
    \E m \in Mem[p][q]: CanDeliver(m, Mem[p][q], Conflict)
        /\ LET
            G == {mm \in Mem[p][q]: \A nn \in Mem[p][q] \ {mm}: mm.s = 3 /\ ~Conflict(mm, nn)}
            D == {m} \cup G
            index == Cardinality(Delivered[p][q])
           IN
            /\ Mem' = [Mem EXCEPT ![p][q] = @ \ D]
            /\ Delivered' = [Delivered EXCEPT ![p][q] = Delivered[p][q] \cup {<<index, D>>}]
            /\ UNCHANGED <<K, PreviousMsgs, ABCast, ProcessComm>>

--------------------------------------------------------------
Step(p, q) ==
    \/ ~(q \in Processes)
    \/ ComputeGroupSeqNumber(p, q)
    \/ GatherGroupsTimestamp(p, q)
    \/ DoDeliver(p, q)

PartitionStep(p) ==
    \E q \in AllProcesses: Step(p, q)

Next ==
    \/ \E self \in Partitions: PartitionStep(self)
    \/ UNCHANGED vars

Spec == Init /\ [][Next]_vars
             /\ WF_vars(\E self \in Partitions: PartitionStep(self))
--------------------------------------------------------------
WasDelivered(p, q, m) ==
    \E <<i, msgs>> \in Delivered[p][q]: \E n \in msgs: m.id = n.id
ExistProcessDeliver(p, m) ==
    \E q \in Processes: WasDelivered(p, q, m)
AllProcessDeliver(p, m) ==
    \A q \in Processes: WasDelivered(p, q, m)
ExistsDeliver(m) ==
    \E p \in m.d: ExistProcessDeliver(p, m)
AllPartitionsDeliver(m) ==
    \A p \in m.d: AllProcessDeliver(p, m)
DeliveredOnlyOnce(p, q, m) == Cardinality({x \in Delivered[p][q]: \E n \in x[2]: n.id = m.id}) = 1
DeliveredIndex(p, m) ==
    (CHOOSE <<i, msgs>> \in UNION {Delivered[p][x]: x \in DOMAIN Delivered[p]}: \E n \in msgs: n.id = m.id)[1]

Validity ==
    <>[]\A m \in ToSend:
        \E p \in m.d: ExistProcessDeliver(p, m)

Agreement ==
    <>[]\A m \in ToSend:
        ExistsDeliver(m) => AllPartitionsDeliver(m)

Integrity ==
    <>[]\A m \in ToSend:
        \A p \in m.d:
            \A q \in Filter(Processes, LAMBDA x: WasDelivered(p, x, m)):
                DeliveredOnlyOnce(p, q, m)

AssertDeliveryOrder(pm, pn, qm, qn) == 
    \/ ((pm < pn) /\ (qm < qn))
    \/ (~(pm < pn) /\ ~(qm < qn))
BothDelivered(p, pp, q, qq, m, n) ==
    /\ WasDelivered(p, q, m) /\ WasDelivered(p, q, n)
    /\ WasDelivered(pp, qq, m) /\ WasDelivered(pp, qq, n)
LHS(p, pp, m, n) ==
    /\ {p, pp} \subseteq (m.d \intersect n.d)
    /\ Conflict(m, n)
    /\ \E q, qq \in Processes:
        BothDelivered(p, pp, q, qq, m, n)
RHS(p, q, m, n) ==
    /\ LET
        pm == DeliveredIndex(p, m)
        pn == DeliveredIndex(p, n)
        qm == DeliveredIndex(q, m)
        qn == DeliveredIndex(q, n)
        IN
        AssertDeliveryOrder(pm, pn, qm, qn)
PartialOrder ==
    []\A m, n \in ToSend:
        \A p, pp \in Partitions:
            LHS(p, pp, m, n) => RHS(p, pp, m, n)

Collision ==
    []\A p \in Partitions:
        \A m, n \in ToSend:
            /\ m.id /= n.id
                /\ p \in (m.d \intersect n.d)
                /\ ExistProcessDeliver(p, m)
                /\ ExistProcessDeliver(p, n)
                /\ Conflict(m, n) => DeliveredIndex(p, m) /= DeliveredIndex(p, n)
==============================================================
