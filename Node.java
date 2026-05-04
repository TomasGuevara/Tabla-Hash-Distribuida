import java.util.*;

class Node {
    static final String ERROR_KEY = "Error: The key %d doesn't have value";
    static final String ERROR_SUCCESSOR = "Error: The node hasn't successor";
    static final int REPLICATION_FACTOR = 3;
    static final int CURRENT_TIME_FACTOR = 5000;
    int id;

    Node successor;
    Node predecessor;

    Map<Integer, String> data;
    Map<Integer, CacheEntry> cache;

    List<Node> fingerTable;

    public Node(int id) {
        this.id = id;
        this.data = new HashMap<>();
        this.cache = new HashMap<>();
        this.fingerTable = new ArrayList<>();
    }

    /*
     * Build the finger table and sort the nodes internally by their IDs
     */
    void buildFingerTable(List<Node> allNodes) {
        fingerTable.clear();

        for (Node node : allNodes) {
            if (node != this) {
                fingerTable.add(node);
            }
        }

        fingerTable.sort(Comparator.comparingInt(n -> n.id));
    }

    /*
     * It looks for the node that follows it,
     * and if there is no node ahead of it,
     * it looks for it at the beginning of the ring
     */
    Node findSuccessor(int key){
        if (successor == null) 
            throw new IllegalStateException(ERROR_SUCCESSOR);
        if (this == successor){
            return this;
        }
        if (inRange(key, this.id, successor.id)){
            return successor;
        } else {
            Node next = closestPrecedingNode(key);
            if (next == this) return successor;
            return next.findSuccessor(key);
        }
    }

    /*
     *  Returns the nearest node preceding the key 
     *  within the ring, using the finger table.
     */
    Node closestPrecedingNode(int key) {
        for (int i = fingerTable.size() - 1; i >= 0; i--) {
            Node finger = fingerTable.get(i);

            if (finger != null && inOpenRange(finger.id, this.id, key)) {
                return finger;
            }
        }
        return this;
    }

    /*
     * Determines whether a key belongs
     * to the circular interval (start, end]
     * within the identifier ring.
     */
    boolean inRange(int key, int start, int end) {
        if (start < end) {
            return key > start && key <= end;
        } else {
            return key > start || key <= end;
        }
    }

    /*
     * Inserts a (key, value) pair into the DHT.
     * First, the node responsible for the key 
     * is located using findSuccessor.
     * Then, the value is stored in that node 
     * and in a number of immediate successors 
     * defined by REPLICATION_FACTOR.
     */
    void put(int key, String value){
        Node responsible = findSuccessor(key);
        int replicas = REPLICATION_FACTOR;

        for (int i = 0; i < replicas; i++){
            responsible.data.put(key, value);
            if (responsible.successor == null){
                String message = String.format(ERROR_KEY, key);
                throw new IllegalArgumentException(message);
            }
            responsible = responsible.successor;
        }
    }

    /*
     * Retrieves the value associated with a key in the DHT.
     * First, it checks the local cache. If the value exists
     * and has not expired, it is returned directly.
     * If not in the cache:
     * If the current node is responsible for the key,
     * it is searched for locally.
     * Otherwise, the query is routed to the responsible node
     * using findSuccessor.
     * The result obtained is cached for a defined period of time.
     */
    String get(int key){
        if (cache.containsKey(key)){
            CacheEntry entry = cache.get(key);
            if (entry.isExpired()){
                cache.remove(key);
            } else {
                return entry.value;
            }
        }

        if (isResponsible(key)) {
            String value = data.get(key);
            if (value == null) {
                throw new IllegalArgumentException(String.format(ERROR_KEY, key));
            }
            return value;
        } else {
            Node next = findSuccessor(key);
            String value = next.get(key);
            cache.put(key, new CacheEntry(value, System.currentTimeMillis() + CURRENT_TIME_FACTOR));
            return value;
        }
    }

    /*
     * Determines whether the current node is responsible for a key.
     * A node is responsible for keys in the range [predecessor, this].
     * This method uses circular logic to correctly handle the ring.
     * If there is no predecessor (initial case), the node is considered
     * responsible for all keys.
     */
    boolean isResponsible(int key) {
    if (predecessor == null) return true;

        if (predecessor.id < id) {
            return key > predecessor.id && key <= id;
        } else {
            return key > predecessor.id || key <= id;
        }
    }

    /*
     * Determines whether a key belongs
     * to the circular interval (start, end)
     * within the identifier ring.
     */
    boolean inOpenRange(int key, int start, int end) {
        if (start < end) {
            return key > start && key < end;
        } else {
            return key > start || key < end;
        }
    }

