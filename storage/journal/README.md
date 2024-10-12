# Journal

The journal package implements a append-only journal with cryptographically-signed entries.
It is universally useful for implementing write-ahead logs, when combined with
other storage and indexing solutions.

## Power Failure Handling

In the event of power failure in the middle of a write, the last entry will have an invalid
signature and also a smaller size than expected by the header, which will cause the
journal to delete these half-written entries upon restart.