import java.math.BigInteger;

/* Represents an entry in the Chord finger table.
 *
 * Each finger entry contains:
 *
 *  - start: the beginning of the interval associated
 *           with this finger, computed as
 *           (n + 2^i) mod 2^M.
 *
 *  - nodeId: the identifier of the successor node
 *            responsible for the start value.
 *
 *  - port: the communication port of the successor node.
 *
 * Finger tables are used by Chord to perform efficient
 * key lookups in O(log N) hops.
 */
class Finger {
    BigInteger start;
    BigInteger nodeId;
    int port;

    public Finger(BigInteger start, BigInteger nodeId, int port) {
        this.start = start;
        this.nodeId = nodeId;
        this.port = port;
    }
}
