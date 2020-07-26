# Fuzzy testing

This folder contains fuzzy tests to verify the Generic Multicast protocol validity, based on 
the [Hashicorp fuzzy test](https://github.com/hashicorp/raft/tree/master/fuzzy). The tests applied here will verify if 
the protocol ensures the reliability to keep working even when facing network failures.


### Test_SequentialCommands

Apply a sequence of commands synchronously on multiple partitions. Iterating over the alphabet letters and writting
one letter a time, sequentially and synchronously, since a single command is issued at a time, at the end of the
test all partitions must be in the same state with the letter `Z`.

