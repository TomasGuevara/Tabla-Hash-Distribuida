import java.math.BigInteger;

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
