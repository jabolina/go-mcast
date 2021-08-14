--------------------    MODULE mcast2    --------------------
EXTENDS Naturals, FiniteSets, Sequences, GBC, Network, Helper

CONSTANTS 
    NPARTITIONS,
    NPROCESS,
    NMESSAGES,
    Conflict(_,_)

Processes == 1 .. NPROCESS
Partitions == 1 .. NPARTITIONS

ChoosePartition == CHOOSE pa \in SUBSET Partitions: Cardinality(pa) > 0
Messages == [m \in 1 .. NMESSAGES |-> {[id |-> m, d |-> Partitions, ts |-> 0, s |-> 0]}]
ToSend == UNION {Messages[i] : i \in 1 .. NMESSAGES}

\* Verify the input values.
ASSUME 
    /\ NPARTITIONS \in (Nat \ {0})

    \* Verify that `NPROCESS` is a natural number greater than 0.
    /\ NPROCESS \in (Nat \ {0})

    \* Verify that the number of messages to exchange is natural
    \* greater than 0.
    /\ NMESSAGES \in (Nat \ {0})

--------------------------------------------------------------
IsEven(x) == x % 2 = 0
ByIdConflict(x, y) == IsEven(x.id) = IsEven(y.id)
AlwaysConflict(x, y) == TRUE
--------------------------------------------------------------

VARIABLES 
    K,
    Mem,
    PreviousMsgs,
    GBCast,
    Delivered,
    ProcessComm

vars == <<
    K,
    Mem,
    PreviousMsgs,
    Delivered,
    GBCast,
    ProcessComm >>

--------------------------------------------------------------

InitProtocol ==
    /\ K = [p \in Partitions |-> [q \in Processes |-> 0]]
    /\ Mem = [p \in Partitions |-> [q \in Processes |-> {}]]
    /\ PreviousMsgs = [p \in Partitions |-> [q \in Processes |-> {}]]
    /\ Delivered = [p \in Partitions |-> [q \in Processes |-> {}]]

InitHelpers ==
    /\ GBCast = [p \in Partitions |-> [q \in Processes |-> Messages]]
    /\ ProcessComm = [p \in Partitions |-> [q \in Processes |-> {}]]

Init == InitProtocol /\ InitHelpers

--------------------------------------------------------------

ComputeGroupSeqNumber(p, q) ==
    /\ HasValue(GBCast[p][q])
    /\ LET
        m == Peek(GBCast[p][q])
       IN
        /\ \/ /\ m.s = 0
              /\ \/ /\ \E n \in PreviousMsgs[p][q]: Conflict(m, n)
                    /\ K' = [K EXCEPT ![p][q] = K[p][q] + 1]
                    /\ PreviousMsgs' = [PreviousMsgs EXCEPT ![p][q] = {m}]
                 \/ /\ \A n \in PreviousMsgs[p][q]: ~Conflict(m, n)
                    /\ PreviousMsgs' = [PreviousMsgs EXCEPT ![p][q] = PreviousMsgs[p][q] \cup {m}]
                    /\ UNCHANGED K
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
        /\ GBCast' = [GBCast EXCEPT ![p][q] = GBDeliver(GBCast[p][q], m)]
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
                  /\ UNCHANGED <<K, PreviousMsgs, GBCast, Delivered>>
               \/ /\ m.ts < Max(timestamps)
                  /\ Mem' = [Mem EXCEPT ![p][q] = InsertOrUpdate(Mem[p][q], n)]
                  /\ GBCast' = [GBCast EXCEPT ![p] = GenericBroadcast(GBCast[p], n, Conflict)]
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
            /\ UNCHANGED <<K, PreviousMsgs, GBCast, ProcessComm>>

--------------------------------------------------------------
Step(p, q) ==
    \/ ComputeGroupSeqNumber(p, q)
    \/ GatherGroupsTimestamp(p, q)
    \/ DoDeliver(p, q)

PartitionStep(p) ==
    \E q \in Processes: Step(p, q)

Next ==
    \/ \E p \in Partitions: PartitionStep(p)
    \/ UNCHANGED vars

Spec == Init /\ [][Next]_vars
             /\ WF_vars(\E p \in Partitions: PartitionStep(p))

--------------------------------------------------------------
WasDelivered(pa, pr, m) ==
    \E <<i, msgs>> \in Delivered[pa][pr]: \E n \in msgs: m.id = n.id
ExistProcessDeliver(p, m) ==
    \E q \in Processes: WasDelivered(p, q, m)
AllProcessDeliver(p, m) ==
    \A q \in Processes: WasDelivered(p, q, m)
ExistsDeliver(m) ==
    \E p \in m.d: ExistProcessDeliver(p, m)
AllPartitionsDeliver(m) ==
    \A p \in m.d: AllProcessDeliver(p, m)
DeliveredOnlyOnce(pa, pr, m) ==
    Cardinality({x \in Delivered[pa][pr]: \E n \in x[2]: n.id = m.id}) = 1
DeliveredIndex(pa, m) ==
    (CHOOSE <<i, msgs>> \in UNION {Delivered[pa][x]: x \in DOMAIN Delivered[pa]}: \E n \in msgs: n.id = m.id)[1]

Validity ==
    <>[]\A m \in ToSend:
        \E p \in m.d: ExistProcessDeliver(p, m)

Agreement ==
    <>[]\A m \in ToSend:
        ExistsDeliver(m) => AllPartitionsDeliver(m)

Integrity ==
    <>[]\A m \in ToSend:
        \A p \in m.d:
            \A q \in {x \in Processes: WasDelivered(p, x, m)}:
                DeliveredOnlyOnce(p, q, m)

AssertDeliveryOrder(pm, pn, qm, qn) == 
    \/ ((pm < pn) /\ (qm < qn))
    \/ (~(pm < pn) /\ ~(qm < qn))
BothDelivered(pa, qa, pr, qr, m, n) ==
    /\ WasDelivered(pa, pr, m) /\ WasDelivered(pa, pr, n)
    /\ WasDelivered(qa, qr, m) /\ WasDelivered(qa, qr, n)
LHS(pa, qa, m, n) ==
    /\ {pa, qa} \subseteq (m.d \intersect n.d)
    /\ Conflict(m, n)
    /\ \E pr, qr \in Processes:
        BothDelivered(pa, qa, pr, qr, m, n)
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
        \A p, q \in Partitions:
            LHS(p, q, m, n) => RHS(p, q, m, n)

Collision ==
    []\A p \in Partitions:
        \A m, n \in ToSend:
            /\ m.id /= n.id
                /\ p \in (m.d \intersect n.d)
                /\ ExistProcessDeliver(p, m)
                /\ ExistProcessDeliver(p, n)
                /\ Conflict(m, n) => DeliveredIndex(p, m) /= DeliveredIndex(p, n)
==============================================================
