# SSTable

SSTable is an implementation of a sorted state table (SSTable), an append-only file containing
key-value entries sorted by key.

It is useful for implementing log structured merge tree (LSM) databases.

## Power Failure Handling

The SSTable file contains a size header that is only written after a successful call to `fsync`
to commit all entries to storage. If a power failure occurs in the middle of an append,
the size will not be updated, which will cause the SSTable to ignore all non-committed entries
upon restart.