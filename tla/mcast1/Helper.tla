--------------------    MODULE Helper    --------------------
LOCAL INSTANCE FiniteSets
LOCAL INSTANCE Naturals
LOCAL INSTANCE Sequences

-------------------------------------------------------------

Max(s) == 
    CHOOSE x \in s: \A y \in s: x >= y

Filter(s, op(_)) ==
    {x \in s: op(x)}

InsertOrUpdate(s, m) ==
    Filter(s, LAMBDA n: n.id /= m.id) \cup {m}

HasReceivedFromAllPartitions(m, network) ==
    m.s = 1 /\ Cardinality(m.d) = Cardinality({v.o : v \in Filter(network, LAMBDA x: x.id = m.id)})

CanDeliver(m, s, op(_, _)) ==
    /\ m.s = 3
    /\ \A n \in s \ {m}:
        /\ m.ts <= n.ts
        /\ \/ ~op(m, n)
           \/ m.id < n.id \/ m.ts < n.ts

=============================================================
