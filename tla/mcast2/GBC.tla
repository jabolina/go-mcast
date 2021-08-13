--------------------    MODULE GBC    --------------------
LOCAL INSTANCE Sequences
LOCAL INSTANCE Naturals
LOCAL INSTANCE FiniteSets

----------------------------------------------------------
(**************************************************************************)
(* TRUE if the sequence s is empty.                                       *)
(**************************************************************************)
IsEmpty(s) ==
    Len(s) = 0

HasValue(s) ==
    Len(s) > 0 /\ Cardinality(Head(s)) > 0

(**************************************************************************)
(* TRUE if the element e is in one of sets inside the sequence s.         *)
(**************************************************************************)
Contains(s, e) ==
    \E i \in 1..Len(s): e \in s[i]

ReplaceAt(s, i, e) ==
    [s EXCEPT ![i] = e]

(**************************************************************************)
(* Return the sequence of sets s without the element e.                   *)
(**************************************************************************)
Remove(s, e) ==
    IF Cardinality(Head(s)) > 1
        THEN ReplaceAt(s, 1, Head(s) \ {e})
        ELSE SubSeq(s, 2, Len(s))

(**************************************************************************)
(* Replaces the element at position i shortening the length of s by one.  *)
(**************************************************************************)
RemoveAt(s, i) ==
    SubSeq(s, 1, i - 1) \o SubSeq(s, i + 1, Len(s))

(**************************************************************************)
(* Remove that last element of the sequence.                              *)
(**************************************************************************)
RemoveLast(s) ==
    SubSeq(s, 1, Len(s) - 1)

(**************************************************************************)
(* The first set of the sequence.                                         *)
(**************************************************************************)
Peek(s) ==
    CHOOSE v \in Head(s): TRUE

(**************************************************************************)
(* Given a sequence of sets s, the element e and the predicate pred       *)
(* if the sequence is empty or exist an element which the predicate is    *)
(* TRUE, then the element will be added to the tail of the sequence.      *)
(* Otherwise the element is inserted into the set at the last position.   *)
(**************************************************************************)
Insert(s, e, pred(_, _)) ==
    IF IsEmpty(s) \/ \E v \in s[Len(s)]: pred(v, e)
        THEN Append(s, {e})
        ELSE Append(RemoveLast(s), s[Len(s)] \cup {e})

==========================================================
