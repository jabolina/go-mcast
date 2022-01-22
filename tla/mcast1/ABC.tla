--------------------    MODULE ABC    --------------------
LOCAL INSTANCE Sequences
LOCAL INSTANCE Naturals
LOCAL INSTANCE FiniteSets

HasValue(s) ==
    Len(s) > 0

AtomicBroadcast(s, e) ==
    [i \in 1..Len(s) |-> Append(s[i], e)]

ABDeliver(s) ==
    Tail(s)

Peek(s) ==
    Head(s)

==========================================================
