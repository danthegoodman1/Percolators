# Percolators

A collection of Percolator clients on top of eisting databases! See each folder for more!

Percolators makes various databases have key-value including transactions with snapshot isolation (like serializable), or a read committed/snapshot hybrid (see each client).

The client has been modified to support:
1. Lock-free snapshot isolation (like serializable but more optimistic in conflict checking)
2. Read committed isolation (ignoring write conflicts and read stamps, uses read cache)
3. Read repeatable isolation (ignoring write conflicts, using same read stamp on retry)
4. Read and write caching (no redundant network calls)