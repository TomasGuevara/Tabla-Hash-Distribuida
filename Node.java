import java.util.*;

class Node {
    public static final String ERROR_KEY = "Error: The key %d doesn't have value";
    public static final String ERROR_SUCCESSOR = "Error: The node hasn't successor";
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

    void buildFingerTable(List<Node> allNodes) {
        fingerTable.clear();

        for (Node node : allNodes) {
            if (node != this) {
                fingerTable.add(node);
            }
        }

        fingerTable.sort(Comparator.comparingInt(n -> n.id));
    }

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

    Node closestPrecedingNode(int key) {
        for (int i = fingerTable.size() - 1; i >= 0; i--) {
            Node finger = fingerTable.get(i);

            if (finger != null && inOpenRange(finger.id, this.id, key)) {
                return finger;
            }
        }
        return this;
    }

    boolean inRange(int key, int start, int end) {
        if (start < end) {
            return key > start && key <= end;
        } else {
            return key > start || key <= end;
        }
    }

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

    boolean isResponsible(int key) {
    if (predecessor == null) return true;

        if (predecessor.id < id) {
            return key > predecessor.id && key <= id;
        } else {
            return key > predecessor.id || key <= id;
        }
    }

    boolean inOpenRange(int key, int start, int end) {
        if (start < end) {
            return key > start && key < end;
        } else {
            return key > start || key < end;
        }
    }
}
