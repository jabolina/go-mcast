-----------------    MODULE Failures    ------------------

LOCAL INSTANCE Sequences
LOCAL INSTANCE Naturals
LOCAL INSTANCE FiniteSets

StillMajority(s, n) == 
    Cardinality(s) >= (n \div 2) + 1

WillFail(p, C, n) ==
    /\ p \in C
    /\ StillMajority(C \ {p}, n)
    /\ CHOOSE x \in BOOLEAN: TRUE

==========================================================