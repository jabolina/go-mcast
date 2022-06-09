--------------------    MODULE mcast1    --------------------

EXTENDS Naturals, FiniteSets, Sequences, ABC, Network, Helper

CONSTANTS 
    NGROUPS,
    NPROCESS,
    NMESSAGES,
    CONFLICTR(_,_)

\* Verify the input values.
ASSUME 
    /\ NGROUPS \in (Nat \ {0})

    \* Verify that `NPROCESS` is a natural number greater than 0.
    /\ NPROCESS \in (Nat \ {0})

    \* Verify that the number of messages to exchange is natural
    \* greater than 0.
    /\ NMESSAGES \in (Nat \ {0})

IsEven(x) == x % 2 = 0
ByIdConflict(x, y) == IsEven(x.id) = IsEven(y.id)
AlwaysConflict(x, y) == TRUE
NeverConflict(x, y) == FALSE

AllProcesses == 1 .. NPROCESS
Partitions == 1 .. NGROUPS

ChoosePartition == CHOOSE p \in SUBSET Partitions: Cardinality(p) > 0
Messages == [m \in 1 .. NMESSAGES |-> [id |-> m, d |-> Partitions, ts |-> 0, s |-> 0]]
ToSend == {Messages[i] : i \in 1 .. NMESSAGES}

VARIABLES 
    K,
    Mem,
    PreviousMsgs,
    ABCast,
    Delivered,
    ProcessComm,
    CorrectProcesses

vars == <<
    K,
    Mem,
    PreviousMsgs,
    Delivered,
    ABCast,
    ProcessComm,
    CorrectProcesses >>

--------------------------------------------------------------
InitProtocol ==
    /\ K = [i \in Partitions |-> [p \in AllProcesses |-> 0]]
    /\ Mem = [i \in Partitions |-> [p \in AllProcesses |-> {}]]
    /\ PreviousMsgs = [i \in Partitions |-> [p \in AllProcesses |-> {}]]
    /\ Delivered = [i \in Partitions |-> [p \in AllProcesses |-> {}]]

InitHelpers ==
    /\ ABCast = [i \in Partitions |-> [p \in AllProcesses |-> Messages]]
    /\ ProcessComm = [i \in Partitions |-> [p \in AllProcesses |-> {}]]
    /\ CorrectProcesses = [i \in Partitions |-> AllProcesses]

Init == InitProtocol /\ InitHelpers

ComputeGroupSeqNumber(p, q) ==
    /\ HasValue(ABCast[p][q])
    /\ LET
        m == Peek(ABCast[p][q])
       IN
        /\ \/ /\ m.s = 0
              /\ \/ /\ \E n \in PreviousMsgs[p][q]: CONFLICTR(m, n)
                    /\ K' = [K EXCEPT ![p][q] = K[p][q] + 1]
                    /\ PreviousMsgs' = [PreviousMsgs EXCEPT ![p][q] = {m}]
                 \/ /\ \A n \in PreviousMsgs[p][q]: ~CONFLICTR(m, n)
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
        /\ ABCast' = [ABCast EXCEPT ![p][q] = ABDeliver(ABCast[p][q])]
        /\ UNCHANGED <<Delivered, CorrectProcesses>>

GatherGroupsTimestamp(p, q) ==
    \E m \in Mem[p][q]: HasReceivedFromAllPartitions(m, ProcessComm[p][q])
        /\ LET
            timestamps == {v.ts : v \in {x \in ProcessComm[p][q]: x.id = m.id}}
            msgs == {x \in ProcessComm[p][q]: x.id = m.id}
            n == [id |-> m.id, d |-> m.d, ts |-> Max(timestamps), s |-> 2]
           IN
            /\ \/ /\ m.ts >= Max(timestamps) \/ \A mm \in PreviousMsgs[p][q]: ~CONFLICTR(m, mm)
                  /\ Mem' = [Mem EXCEPT ![p][q] = InsertOrUpdate(Mem[p][q], [id |-> m.id, d |-> m.d, ts |-> m.ts, s |-> 3])]
                  /\ UNCHANGED <<K, PreviousMsgs, ABCast, Delivered>>
               \/ /\ m.ts < Max(timestamps)
                  /\ Mem' = [Mem EXCEPT ![p][q] = InsertOrUpdate(Mem[p][q], n)]
                  /\ ABCast' = [ABCast EXCEPT ![p] = AtomicBroadcast(ABCast[p], n)]
                  /\ UNCHANGED <<K, PreviousMsgs, Delivered>>
            /\ ProcessComm' = [ProcessComm EXCEPT ![p][q] = @ \ msgs]
            /\ UNCHANGED CorrectProcesses

DoDeliver(p, q) ==
    \E m \in Mem[p][q]: CanDeliver(m, Mem[p][q], CONFLICTR)
        /\ LET
            G == {mm \in Mem[p][q]: \A nn \in Mem[p][q] \ {mm}: mm.s = 3 /\ ~CONFLICTR(mm, nn)}
            D == {m} \cup G
            index == Cardinality(Delivered[p][q])
           IN
            /\ Mem' = [Mem EXCEPT ![p][q] = @ \ D]
            /\ Delivered' = [Delivered EXCEPT ![p][q] = Delivered[p][q] \cup {<<index, D>>}]
            /\ UNCHANGED <<K, PreviousMsgs, ABCast, ProcessComm, CorrectProcesses>>

MaybeCrash(p, q) ==
    /\ q \in CorrectProcesses[p]
    /\ Cardinality(CorrectProcesses[p] \ {q}) >= (NPROCESS \div 2) + 1
    /\ CorrectProcesses' = [CorrectProcesses EXCEPT ![p] = @ \ {q}]
    /\ UNCHANGED <<K, Mem, PreviousMsgs, Delivered, ABCast, ProcessComm>>

NoOpCrashed(p, q) ==
    /\ ~(q \in CorrectProcesses[p])
    /\ UNCHANGED vars

--------------------------------------------------------------
Step(p, q) ==
    \*\/ MaybeCrash(p, q)
    \*\/ NoOpCrashed(p, q)
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
    \E q \in AllProcesses: WasDelivered(p, q, m)
AllProcessDeliver(p, m) ==
    \A q \in CorrectProcesses[p]: WasDelivered(p, q, m)
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
            \A q \in Filter(CorrectProcesses[p], LAMBDA x: WasDelivered(p, x, m)):
                DeliveredOnlyOnce(p, q, m)

AssertDeliveryOrder(pm, pn, qm, qn) == 
    \/ ((pm < pn) /\ (qm < qn))
    \/ (~(pm < pn) /\ ~(qm < qn))
BothDelivered(p, pp, q, qq, m, n) ==
    /\ WasDelivered(p, q, m) /\ WasDelivered(p, q, n)
    /\ WasDelivered(pp, qq, m) /\ WasDelivered(pp, qq, n)
LHS(p, pp, m, n) ==
    /\ {p, pp} \subseteq (m.d \intersect n.d)
    /\ CONFLICTR(m, n)
    /\ \E q, qq \in AllProcesses:
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
                /\ CONFLICTR(m, n) => DeliveredIndex(p, m) /= DeliveredIndex(p, n)
==============================================================
