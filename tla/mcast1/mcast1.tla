--------------------    MODULE mcast1    --------------------

EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS 
    NPARTITIONS,
    NPROCESS,
    NMESSAGES,
    Conflict(_,_)

Processes == 1 .. NPROCESS
Partitions == 1 .. NPARTITIONS

ChoosePartition == CHOOSE pa \in SUBSET Partitions: Cardinality(pa) > 0
Messages == [m \in 1 .. NMESSAGES |-> [id |-> m, d |-> Partitions, ts |-> 0, s |-> 0]]
ToSend == {Messages[i] : i \in 1 .. NMESSAGES}
OnlyIncluded(pa) == {m \in Messages: pa \in m.d}

IsEven(x) == x % 2 = 0
ByIdConflict(x, y) == IsEven(x.id) = IsEven(y.id)
AlwaysConflict(x, y) == TRUE

Max(S) == CHOOSE x \in S: \A y \in S : x >= y

UpdateMessageState(S, m) == IF Cardinality(S) = 0 THEN {m} ELSE {IF n.id /= m.id THEN n ELSE m : n \in S}

\* Verify the input values.
ASSUME 
    /\ NPARTITIONS \in (Nat \ {0})

    \* Verify that `NPROCESS` is a natural number greater than 0.
    /\ NPROCESS \in (Nat \ {0})

    \* Verify that the number of messages to exchange is natural
    \* greater than 0.
    /\ NMESSAGES \in (Nat \ {0})

VARIABLES 
    K,
    Mem,
    PreviousMsgs,
    ABCast,
    Delivered,
    Network

vars == <<
    K,
    Mem,
    PreviousMsgs,
    Delivered,
    ABCast,
    Network >>

--------------------------------------------------------------
InitProtocol ==
    /\ K = [i \in Partitions |-> [p \in Processes |-> 0]]
    /\ Mem = [i \in Partitions |-> [p \in Processes |-> {}]]
    /\ PreviousMsgs = [i \in Partitions |-> [p \in Processes |-> {}]]
    /\ Delivered = [i \in Partitions |-> [p \in Processes |-> {}]]

InitHelpers ==
    /\ ABCast = [i \in Partitions |-> [p \in Processes |-> Messages]]
    /\ Network = [i \in Partitions |-> [p \in Processes |-> {}]]

Init == InitProtocol /\ InitHelpers

ComputeGroupSeqNumber(p, q) ==
    /\ Len(ABCast[p][q]) > 0
    /\ LET
        m == Head(ABCast[p][q])
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
                    /\ Mem' = [Mem EXCEPT ![p][q] = (@ \ {m}) \cup {[id |-> m.id, d |-> m.d, ts |-> K'[p][q], s |-> 1]}]
                    /\ Network' = [pp \in Partitions |-> IF pp \in m.d
                            THEN [qq \in Processes |-> Network[pp][qq] \cup {[id |-> m.id, d |-> m.d, ts |-> K'[p][q], s |-> 1, o |-> p]}]
                            ELSE Network[pp]]
                 \/ /\ m.s = 2
                    /\ \/ /\ m.ts > K[p][q]
                          /\ K' = [K EXCEPT ![p][q] = m.ts]
                          /\ PreviousMsgs' = [PreviousMsgs EXCEPT ![p][q] = {}]
                       \/ /\ m.ts <= K[p][q]
                          /\ UNCHANGED <<K, PreviousMsgs>>
                    /\ Mem' = [Mem EXCEPT ![p][q] = UpdateMessageState(Mem[p][q], [id |-> m.id, d |-> m.d, ts |-> m.ts, s |-> 3])]
                    /\ UNCHANGED Network
           \/ /\ Cardinality(m.d) = 1
              /\ Mem' = [Mem EXCEPT ![p][q] = (@ \ {m}) \cup {[id |-> m.id, d |-> m.d, ts |-> K'[p][q], s |-> 3]}]
              /\ UNCHANGED Network
        /\ ABCast' = [ABCast EXCEPT ![p][q] = Tail(ABCast[p][q])]
        /\ UNCHANGED Delivered

GatherGroupsTimestamp(p, q) ==
    \E m \in Mem[p][q]: 
        /\ m.s = 1
        /\ Cardinality(m.d) = Cardinality({v.o : v \in {x \in Network[p][q]: x.id = m.id}})
        /\ LET
            timestamps == {v.ts : v \in {x \in Network[p][q]: x.id = m.id}}
            msgs == {x \in Network[p][q]: x.id = m.id}
           IN
            /\ \/ /\ m.ts >= Max(timestamps)
                  /\ Mem' = [Mem EXCEPT ![p][q] = UpdateMessageState(Mem[p][q], [id |-> m.id, d |-> m.d, ts |-> m.ts, s |-> 3])]
                  /\ UNCHANGED <<K, PreviousMsgs, ABCast, Delivered>>
               \/ /\ m.ts < Max(timestamps)
                  /\ Mem' = [Mem EXCEPT ![p][q] = UpdateMessageState(Mem[p][q], [id |-> m.id, d |-> m.d, ts |-> Max(timestamps), s |-> 2])]
                  /\ ABCast' = [ABCast EXCEPT ![p] =
                        [qq \in Processes |-> Append(ABCast[p][qq], [id |-> m.id, d |-> m.d, ts |-> Max(timestamps), s |-> 2])]]
                  /\ UNCHANGED <<K, PreviousMsgs, Delivered>>
            /\ Network' = [Network EXCEPT ![p][q] = @ \ msgs]

DoDeliver(p, q) ==
    \E m \in Mem[p][q]:
        /\ m.s = 3
        /\ \A n \in Mem[p][q] \ {m}:
            /\ m.ts <= n.ts
            /\ \/ ~Conflict(m, n)
               \/ m.id < n.id \/ m.ts < n.ts
        /\ LET
            G == {mm \in Mem[p][q]: \A nn \in Mem[p][q] \ {mm}: mm.s = 3 /\ ~Conflict(mm, nn)}
            D == {m} \cup G
            index == Cardinality(Delivered[p][q])
           IN
            /\ Mem' = [Mem EXCEPT ![p][q] = @ \ D]
            /\ Delivered' = [Delivered EXCEPT ![p][q] = Delivered[p][q] \cup {<<index, D>>}]
            /\ UNCHANGED <<K, PreviousMsgs, ABCast, Network>>

--------------------------------------------------------------
Step(pa, pr) ==
    \/ ComputeGroupSeqNumber(pa, pr)
    \/ GatherGroupsTimestamp(pa, pr)
    \/ DoDeliver(pa, pr)

PartitionStep(pa) ==
    \E pr \in Processes: Step(pa, pr)

Next ==
    \/ \E self \in Partitions: PartitionStep(self)
    \/ UNCHANGED vars

Spec == Init /\ [][Next]_vars
             /\ WF_vars(\E self \in Partitions: PartitionStep(self))
--------------------------------------------------------------
WasDelivered(pa, pr, m) ==
    \E <<i, msgs>> \in Delivered[pa][pr]: \E n \in msgs: m.id = n.id
ExistDeliver(pa, m) ==
    \E pr \in Processes: WasDelivered(pa, pr, m)
UnionPartitionDeliver(pa, m) ==
    UNION {Delivered[pa][x]: x \in DOMAIN Delivered[pa]}
DeliveredIndex(pa, m) ==
    (CHOOSE <<i, msgs>> \in UnionPartitionDeliver(pa, m): \E n \in msgs: n.id = m.id)[1]

Validity ==
    \A m \in ToSend:
        \E <<pa, pr>> \in Partitions \X Processes:
                pa \in m.d ~> \E ppa \in m.d : \E ppr \in Processes: WasDelivered(ppa, ppr, m)

Agreement ==
    \A m \in ToSend:
        \A <<pa, pr>> \in Partitions \X Processes:
            WasDelivered(pa, pr, m) ~> \A ppa \in m.d: \E ppr \in Processes: WasDelivered(ppa, ppr, m)

DeliveredOnlyOnce(pa, pr, m) == Cardinality({x \in Delivered[pa][pr]: \E n \in x[2]: n.id = m.id}) = 1
Integrity ==
    \A m \in ToSend:
        \A <<pa, pr>> \in Partitions \X Processes:
            pa \in m.d ~> \A ppa \in Partitions: 
                            \E ppr \in Processes: 
                                WasDelivered(ppa, ppr, m) /\ DeliveredOnlyOnce(ppa, ppr, m)


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
                /\ ExistDeliver(p, m)
                /\ ExistDeliver(p, n)
                /\ Conflict(m, n) => DeliveredIndex(p, m) /= DeliveredIndex(p, n)
==============================================================
