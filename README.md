# Distributed Hash Table (DHT) - Chord Implementation in Java

## Overview

This project is an implementation of a **Distributed Hash Table (DHT)** based on the **Chord protocol**, developed as the final project for the *Telecommunications and Distributed Systems* course of UNRC **Universidad Nacional de Río Cuarto**.

The implementation provides a decentralized peer-to-peer ring where each node is responsible for a subset of the key space. Keys and node identifiers are generated using the **SHA-1** hash function, closely following the original Chord paper.

---

## Features

* SHA-1 based node identifiers.
* SHA-1 based key hashing.
* Dynamic node join.
* Automatic key redistribution after node insertion.
* Successor and predecessor maintenance.
* Finger Table routing.
* O(log N) key lookup.
* Data replication on successor node.
* Local cache with expiration.
* Periodic finger table maintenance.
* Node failure detection using heartbeat (PING/PONG).
* Successor recovery after node failure.

---

## Chord Ring

Each node receives an identifier generated as

```
SHA1(host:port) mod 2^M
```

where

```
M = 8
```

during development for easier debugging.

The implementation is compatible with

```
M = 160
```

which corresponds to the complete SHA-1 identifier space.

---

## Finger Table

Each node maintains a Finger Table following the original Chord definition.

For every entry:

```
finger[i] = successor(
    (n + 2^i) mod 2^M
)
```

The Finger Table allows routing requests in logarithmic time instead of forwarding every request through the immediate successor.

---

## Project Structure

```
Node.java
    Main Chord node implementation.

Finger.java
    Represents a Finger Table entry.

HashUtil.java
    SHA-1 hashing utilities.

CacheEntry.java
    Local cache implementation with expiration.
```

---

## Supported Operations

### Connect a node

```
CONNECT <port>
```

Example

```
CONNECT 5001
```

---

### Store a value

```
PUT <key> <value>
```

Example

```
PUT Joseph Pedro
```

---

### Retrieve a value

```
GET <key>
```

Example

```
GET Joseph
```

---

### Print Finger Table

```
PRINTFINGERTABLE
```

---

### Print successor

```
PRINTSUCCESSOR
```

---

### Print predecessor

```
PRINTPREDECESSOR
```

---

## Periodic Maintenance

Each node periodically executes:

* `fixFingers()`

  * Updates one Finger Table entry at a time.

* `stabilize()`

  * Detects failed successors.
  * Selects a new successor when necessary.

These maintenance operations emulate the stabilization protocol described in the Chord paper.

---

## Routing Example

Suppose the ring contains the following node identifiers

```
11
68
142
177
215
217
```

Looking up key

```
250
```

may follow a path similar to

```
142
 ↓
215
 ↓
217
 ↓
11
```

instead of traversing every node in the ring.

---

## Technologies

* Java
* TCP sockets
* Multithreading
* SHA-1
* BigInteger

---

## References

* Ion Stoica, Robert Morris, David Karger, M. Frans Kaashoek, Hari Balakrishnan. **Chord: A Scalable Peer-to-Peer Lookup Service for Internet Applications.**
* Telecommunications and Distributed Systems Course Project from UNRC **Universidad Nacional de Río Cuarto**.

