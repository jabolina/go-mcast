--------------------    MODULE Network    --------------------

LOCAL INSTANCE Sequences
LOCAL INSTANCE Naturals
LOCAL INSTANCE FiniteSets

--------------------------------------------------------------
SendToPartition(s, m) ==
    [p \in DOMAIN s |-> s[p] \cup {m}]

Send(s, m) ==
    [p \in DOMAIN s |-> IF p \in m.d THEN SendToPartition(s[p], m) ELSE s[p]]

==============================================================
