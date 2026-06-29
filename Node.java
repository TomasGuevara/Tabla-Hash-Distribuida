import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.math.BigInteger;
import java.io.*;
import java.net.*;
import java.util.*;

public class Node {

    static final String ERROR_KEY =
        "Error: The key %d doesn't have value";

    static final String WARNING_CACHE =
        "CACHE Entry for key %d expired and was removed.";

    static final int CURRENT_TIME_FACTOR = 5000;

    /* Number of bits used to represent identifiers
     * in the Chord ring. During development M = 8
     * is used for easier debugging, although the
     * implementation supports larger values (e.g. 160).
     */
    static final int M = 8;

    BigInteger id;
    int port;
    int fingerReviewed;

    int successorPort;
    BigInteger successorId;
    int predecessorPort;
    BigInteger predecessorId;

    BigInteger ringSize = new BigInteger("256");
    int pingNumber;

    Map<BigInteger, String> data;
    Map<BigInteger, CacheEntry> cache;

    List<Finger> fingerTable;

    static ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // =========================
    // CONSTRUCTOR
    // =========================
    /* Generate node identifier using SHA-1(host:port)
     * and map it into the identifier space [0, 2^M - 1].
     */
    public Node(String host, int port) {

        this.id = HashUtil.sha1(host+":"+port).mod(ringSize);
	this.fingerTable = new ArrayList<>();
        this.port = port;

        this.successorPort = 0;
	this.successorId = BigInteger.ZERO;
        this.predecessorPort = 0;
        this.predecessorId = BigInteger.ZERO;
	this.fingerReviewed = 0;
	this.pingNumber = 0;

        this.data = new HashMap<>();
        this.cache = new HashMap<>();
	this.fingerTable = new ArrayList<>();
    }

    // =========================
    // SERVER
    // =========================
    /* Starts the TCP server associated with this node.
     *
     * The server listens for incoming requests from
     * clients and other nodes participating in the
     * Chord ring.
     *
     * Each incoming connection is handled in a
     * separate thread in order to allow concurrent
     * processing of multiple requests.
     */
    public void startServer() {

        try {

            ServerSocket serverSocket =
                new ServerSocket(port);

            System.out.println(
                "[NODE " + id + "] Listening on port " + port
            );

            while (true) {

                Socket client = serverSocket.accept();

                new Thread(() -> handleClient(client)).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // =========================
    // HANDLE REQUESTS
    // =========================
    /* Processes requests received through a TCP connection.
     *
     * Requests may come either from external clients
     * (GET/PUT operations) or from other nodes in the
     * Chord overlay network (routing, stabilization,
     * replication and maintenance operations).
     *
     * The request protocol is text-based and each
     * message starts with an operation code followed
     * by its corresponding arguments.
     */
    void handleClient(Socket client) {

        try {

            BufferedReader in = new BufferedReader(
                new InputStreamReader(client.getInputStream())
            );

            PrintWriter out = new PrintWriter(
                client.getOutputStream(),
                true
            );

            String request = in.readLine();

            String[] parts = request.split(" ");

            // GETCONNECTION
            if (parts[0].equals("GETCONNECTION")) {

                int receivedPort =
                    Integer.parseInt(parts[1]);

		BigInteger receivedId =
                    new BigInteger(parts[2]);

                String response =
                    getConnection(receivedPort, receivedId);

                out.println(response);
            }

	    // GETCHANGECONNECTION
            if (parts[0].equals("GETCHANGECONNECTION")) {

                int receivedPort =
                    Integer.parseInt(parts[1]);

		BigInteger receivedId =
                    new BigInteger(parts[2]);

                String response =
                    getChangeConnection(receivedPort, receivedId);

                out.println(response);
            }

	    // CHANGEKEYVALUE
            if (parts[0].equals("CHANGEKEYVALUE")) {

                changeKeyValue();
            }

            // PUT
            else if (parts[0].equals("PUT")) {

                String key = parts[1];

                String value = parts[2];

		String response = put(key, value, false);

                out.println(response);
            }

            // GET
            else if (parts[0].equals("GET")) {

                String key = parts[1];

                String value = get(key, false);

                out.println(value);
            }

	    // PUTREPLICATION
	    else if (parts[0].equals("PUTREPLICATION")){
		BigInteger key = new BigInteger(parts[1]);
		
		String value = parts[2];

                data.put(key, value);
	    }

	    // FINDSUCCESSOR
            else if (parts[0].equals("FINDSUCCESSOR")){
		BigInteger key = new BigInteger(parts[1]);
		
		Finger value = findSuccessor(key);

                out.println(value.start + "," + value.nodeId + "," + value.port); 
	    }

	    // UPDATEFINGER
	    else if (parts[0].equals("UPDATEFINGER")) {

                BigInteger sourceId = new BigInteger(parts[1]);

                if (!sourceId.equals(this.id)) {

                    buildFingerTable();

                    sendUpdateFinger(this.successorPort, sourceId);
                }
            }

	    // FINDNEXTSUCCESSOR
            else if (parts[0].equals("FINDNEXTSUCCESSOR")) {

		BigInteger givenPredecessorId = new BigInteger(parts[1]);

                BigInteger givenId = new BigInteger(parts[2]);

		int givenPort = Integer.parseInt(parts[3]);

                String response = notify(givenPredecessorId, givenId, givenPort);

                out.println(response);
            }

	    // PING
	    else if (parts[0].equals("PING")) {

                out.println("PONG");
            }

            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // =========================
    // CONNECTION
    // =========================
    /* Processes a join request from a new node.
     *
     * If this node is the appropriate predecessor for
     * the joining node, the ring structure is updated
     * locally. Otherwise, the request is forwarded
     * through the Chord overlay using the routing
     * information stored in the finger table.
     *
     * @param portReceived TCP port of the joining node.
     * @param idReceived Identifier of the joining node.
     * @return Information required by the new node to
     *         initialize its predecessor and successor.
     */
    public String getConnection(int portReceived, BigInteger idReceived) {

        if (this.successorPort == 0) {

            this.successorPort = portReceived;

	    this.successorId = idReceived;

            this.predecessorPort = portReceived;

            this.predecessorId = idReceived;

            System.out.println(
                "[NODE " + id + "] Connected with "
                + this.successorPort
            );

            return 0 + "," + this.port + "," + this.id;
        } else {

	    if (this.successorId.compareTo(idReceived) > 0 ||
	        ((this.predecessorId.compareTo(idReceived) < 0) && (this.predecessorId.compareTo(this.id) > 0)) ||
		((this.predecessorId.compareTo(idReceived) > 0) && this.id.compareTo(idReceived) > 0)){

	        String value = sendChangeConnection(portReceived, idReceived);

	        return value;
	    } else {

                Finger closestPrecedingNode = closestPrecedingFinger(idReceived);
		
		try{

		    Socket socket =
                        new Socket("localhost", closestPrecedingNode.port);

                    PrintWriter out = new PrintWriter(
                        socket.getOutputStream(),
                        true
                    );

                    BufferedReader in = new BufferedReader(
                        new InputStreamReader(
                            socket.getInputStream()
                        )
                    );

                    out.println("GETCONNECTION " + portReceived + " " + idReceived);

                    String response = in.readLine();

		    socket.close();

		    return response;
		} catch (Exception e) {
                
		    return "Error";
		}
	    }
        }
    }

    /* Initiates the join operation.
     *
     * The node contacts an existing participant of
     * the Chord ring and requests to be inserted
     * into the overlay network.
     *
     * Once the insertion point is found, predecessor
     * and successor references are initialized and
     * the distributed key space is rebalanced.
     *
     * @param port Port of any node already belonging
     *             to the Chord ring.
     */
    public void sendConnection(int port) {

	if (this.predecessorPort == 0) {

            try {

                Socket socket =
                    new Socket("localhost", port);

                PrintWriter out = new PrintWriter(
                    socket.getOutputStream(),
                    true
                );

                BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                        socket.getInputStream()
                    )
                );

                out.println("GETCONNECTION " + this.port + " " + this.id);

                String response = in.readLine();

                String[] parts = response.split(",");

		socket.close();

	        if ("0".equals(parts[0])){

		    this.successorPort =
                        Integer.parseInt(parts[1]);

                    this.successorId =
                        new BigInteger(parts[2]);

                    this.predecessorPort =
                        Integer.parseInt(parts[1]);

                    this.predecessorId =
                        new BigInteger(parts[2]);

		    System.out.println(
                        "[NODE " + id + "] Connected with "
                            + predecessorPort
                    );
	        } else {
		
		    this.successorPort =
                        Integer.parseInt(parts[3]);

                    this.successorId =
                        new BigInteger(parts[4]);

                    this.predecessorPort =
                        Integer.parseInt(parts[1]);

                    this.predecessorId =
                        new BigInteger(parts[2]);

		    socket =
                        new Socket("localhost", this.successorPort);

                    out = new PrintWriter(
                        socket.getOutputStream(),
                        true
                    );

		    out.println("CHANGEKEYVALUE");

                    socket.close();

                    socket =
                        new Socket("localhost", this.predecessorPort);

                    out = new PrintWriter(
                        socket.getOutputStream(),
                        true
                    );

		    out.println("CHANGEKEYVALUE");

                    socket.close();

		    System.out.println(
                        "[NODE " + id + "] Connected between Nodes "
                            + parts[2] + " and " + parts[4]
                    );

		}

		updateAllFingerTables();

            } catch (Exception e) {

                e.printStackTrace();
            }

	}else{

	    System.out.println( "Error: node already has predecessor" );
	}
    }

    // =========================
    // CHANGE CONNECTION
    // =========================
    /* Updates the predecessor or successor of the current node
     * when a new node is inserted into the ring.
     *
     * Depending on the identifier of the joining node, the current
     * node decides whether the new node should become its predecessor
     * or successor.
     *
     * @param portReceived Port of the joining node.
     * @param idReceived Identifier of the joining node.
     * @return Current node information in the format "port,id".
     */
    public String getChangeConnection(int portReceived, BigInteger idReceived) {

        if (this.id.compareTo(idReceived) > 0) {

            this.predecessorPort = portReceived;

            this.predecessorId = idReceived; 

            System.out.println(
                "[NODE " + id + "] Connected with new predecessor Node "
                + idReceived 
            );

            return this.port + "," + this.id;

        } else {

	    this.successorPort = portReceived;

            this.successorId = idReceived;

            System.out.println(
                "[NODE " + id + "] Connected with new successor Node "
                + idReceived 
            );

            return this.port + "," + this.id; 

        }
    }

    /* Inserts a new node between two existing nodes in the ring.
     *
     * This method forwards the request to either the predecessor
     * or successor depending on the identifier ordering until the
     * correct insertion position is found.
     *
     * Once the position is found, local predecessor/successor
     * references are updated and the information required by the
     * joining node is returned.
     *
     * @param port Port of the joining node.
     * @param idReceived Identifier of the joining node.
     * @return A string containing predecessor and successor
     *         information for the joining node.
     */
    public String sendChangeConnection(int port, BigInteger idReceived) {

        try {
	    boolean isSuccessor = this.id.compareTo(idReceived) < 0 && this.successorId.compareTo(idReceived) > 0;

	    Socket socket;

	    if (isSuccessor){
                socket =
                    new Socket("localhost", this.successorPort);
	    } else {
		socket =
                    new Socket("localhost", this.predecessorPort);
	    }

            PrintWriter out = new PrintWriter(
                socket.getOutputStream(),
                    true
            );

            BufferedReader in = new BufferedReader(
                new InputStreamReader(
                    socket.getInputStream()
                )
            );

	    out.println("GETCHANGECONNECTION " + port + " " + idReceived);

            String response = in.readLine();

            String[] parts = response.split(",");

	    String value = "";

	    if (isSuccessor){

		this.successorPort = port;

                this.successorId = idReceived;

		System.out.println(
                "[NODE " + id + "] Connected with successor Node"
                    + idReceived
                );
		
		value = "1," + this.port + "," + this.id 
		    + "," + parts[0] + "," + parts[1];
	    } else {

                this.predecessorPort = port;

                this.predecessorId = idReceived;

		System.out.println(
                "[NODE " + id + "] Connected with predecessor Node"
                    + idReceived
                );

		value = "2," + parts[0] + "," + parts[1] 
		    + "," + this.port + "," + this.id; 

	    }

            socket.close();

	    return value;

        } catch (Exception e) {
            return "ERROR";
        }
    }

    // =========================
    // CHANGE KEY VALUE
    // =========================
    /* Redistributes locally stored key-value pairs.
     *
     * This method is invoked when a new node joins the ring.
     * Since the insertion of a new node changes the key ranges
     * for which nodes are responsible, every locally stored pair
     * is reinserted into the DHT.
     *
     * The current data map is temporarily copied and cleared.
     * Each pair is then inserted again using the normal PUT
     * mechanism, allowing the DHT to automatically relocate
     * the pair to its new responsible node.
     */
    public void changeKeyValue() {
	Map<BigInteger, String> dumpData = new HashMap<>(data);
	data = new HashMap<>();
        for (Map.Entry<BigInteger, String> entry : dumpData.entrySet()) {
		put(entry.getKey().toString(16), entry.getValue(), true);
	}
    }

    // =========================
    // PUT
    // =========================
    /* Stores a key-value pair in the DHT.
     *
     * If the current node is responsible for the key,
     * the pair is stored locally and replicated on the
     * immediate successor to provide fault tolerance.
     *
     * Otherwise, the request is forwarded to the closest
     * preceding node obtained from the finger table,
     * reducing lookup complexity from O(n) to O(log n).
     *
     * @param stringKey Original key provided by the client.
     * @param value Associated value.
     * @param isHashed Indicates whether the key has already
     *                 been hashed.
     *
     * @return A message indicating where the key was stored.
     */
    public String put(String stringKey, String value, boolean isHashed) {

	BigInteger key = BigInteger.ZERO;

	if(isHashed){

	    key = new BigInteger(stringKey);
	} else{

	    key = HashUtil.sha1(stringKey).mod(ringSize);
	}

        if (isResponsible(key)) {

            data.put(key, value);

	    if (this.successorPort != 0 && this.successorPort != this.port){
                
		try {
		    Socket socket =
                        new Socket("localhost", this.successorPort);

                    PrintWriter out = new PrintWriter(
                        socket.getOutputStream(),
                        true
                    );

                    out.println("PUTREPLICATION " + key + " " + value);

                    socket.close();

		} catch (Exception e) {
                    e.printStackTrace();
                }
	    }

            return ("[NODE " + id + "] Stored key "+ key);

        } else {
	
	    Finger next = closestPrecedingFinger(key);

	    System.out.println("start:" + next.start + " node:" + next.nodeId + " port:" + next.port);

            return sendPutNext(stringKey, value, next);
        }
    }

    // =========================
    // GET
    // =========================
    /* Retrieves a value associated with a given key.
     *
     * The method first checks the local cache when the
     * request originates from a client. If the cache
     * entry is valid, the value is returned immediately.
     *
     * Otherwise, if the current node is responsible for
     * the key, the value is retrieved from local storage.
     *
     * If the node is not responsible, the request is
     * forwarded through the finger table to the closest
     * preceding node, achieving logarithmic lookup time.
     *
     * Retrieved remote values are cached locally to
     * accelerate future requests.
     *
     * @param stringKey Original key provided by the client.
     * @param isLocal Indicates whether the request originated
     *                locally or was forwarded by another node.
     *
     * @return The value associated with the key.
     */
    public String get(String stringKey, boolean isLocal) {
        // CACHE
	BigInteger key = HashUtil.sha1(stringKey).mod(ringSize);
	
        if (isLocal && cache.containsKey(key)) {

            CacheEntry entry = cache.get(key);

            if (entry.isExpired()) {

                cache.remove(key);
  
                return (
                    String.format(WARNING_CACHE, key)
                );

            } else {

                System.out.println(
                    "[CACHE] Hit for key " + key
                );

                return entry.value;
            }
        }

        // RESPONSIBLE
        if (isResponsible(key)) {

            String value = data.get(key);

            if (value == null) {

                throw new IllegalArgumentException(
                    String.format(ERROR_KEY, key)
                );
            }

            return value;

        } else {

            Finger next = closestPrecedingFinger(key);

	    System.out.println("start:" + next.start + " node:" + next.nodeId + " port:" + next.port);

            String value =
                sendGetFinger(next.port, stringKey);

            if (isLocal){
	        cache.put(
                    key,
                    new CacheEntry(
                        value,
                        System.currentTimeMillis()
                        + CURRENT_TIME_FACTOR
                    )
                );
	    }

            return value;
        }
    }

    // =========================
    // RESPONSIBILITY
    // =========================
    /* Determines whether the current node is responsible
     * for storing a given key.
     *
     * In Chord, a node n is responsible for all keys in the
     * interval (predecessor(n), n].
     *
     * Since the identifier space is circular, two cases must
     * be considered:
     *
     * 1. Normal interval:
     *      predecessor < current node
     *
     * 2. Circular interval (ring wrap-around):
     *      predecessor > current node
     *
     * Example:
     *      predecessor = 215
     *      current node = 11
     *
     * In this case, the node is responsible for:
     *      (215, 255] U [0, 11]
     *
     * @param key Identifier of the key being queried.
     *
     * @return true if the current node is responsible
     *         for the key; false otherwise.
     */
    boolean isResponsible(BigInteger key) {

        if (predecessorPort == 0) {
            return true;
        }

        if (predecessorId.compareTo(id) < 0) {

            return key.compareTo(predecessorId) > 0
                && key.compareTo(id) <= 0;

        } else {

            return key.compareTo(predecessorId) > 0
                || key.compareTo(id) <= 0;
        }
    }

    // =========================
    // FORWARD GET
    // =========================
    /* Forwards a GET operation to another node.
     *
     * This method is used when the current node is not
     * responsible for the requested key. The request is
     * sent to the node selected by the routing algorithm
     * (typically obtained from the finger table).
     *
     * @param port Destination node port.
     * @param key Original client key.
     *
     * @return The value associated with the key or an
     *         error message if communication fails.
     */
    String sendGetFinger(int port, String key) {

        try {

            Socket socket =
                new Socket("localhost", port);

            PrintWriter out = new PrintWriter(
                socket.getOutputStream(),
                true
            );

            BufferedReader in = new BufferedReader(
                new InputStreamReader(
                    socket.getInputStream()
                )
            );

            out.println("GET " + key);

            String response = in.readLine();

            socket.close();

            return response;

        } catch (IOException e) {

            return "ERROR";
        }
    }

    // =========================
    // CLOSEST PRECEDING FINGER
    // =========================
    /* Searches the finger table for the closest node
     * preceding the given key identifier.
     *
     * The finger table is scanned in reverse order
     * (from the farthest entry to the closest one)
     * in order to maximize the routing progress.
     *
     * According to the Chord algorithm, the selected
     * node must satisfy:
     *
     *      currentNode < finger < key
     *
     * Choosing the farthest valid finger guarantees
     * logarithmic search complexity O(log n).
     *
     * If no suitable finger exists, the immediate
     * successor is returned.
     *
     * @param key Identifier being searched.
     *
     * @return The closest preceding finger entry.
     */
    Finger closestPrecedingFinger(BigInteger key) {

        for (int i = fingerTable.size() - 1; i >= 0; i--) {

            Finger finger = fingerTable.get(i);

            if (inRange(finger.nodeId, this.id, key)) {
                return finger;
            }
        }

	Finger successor = new Finger(key, this.successorId, this.successorPort);

        return successor;
    }

    // =========================
    // FORWARD PUT
    // =========================
    /* Forwards a PUT operation to another node.
     *
     * This method is invoked when the current node is not
     * responsible for storing a given key. The request is
     * forwarded to the node selected by the routing algorithm
     * (typically obtained from the finger table).
     *
     * @param key Original key provided by the client.
     * @param value Value associated with the key.
     * @param next Finger table entry representing the next
     *             node in the lookup path.
     *
     * @return The response generated by the responsible node
     *         or an error message if communication fails.
     */
    String sendPutNext(String key, String value, Finger next) {

        try {

            Socket socket =
                new Socket("localhost", next.port);

            PrintWriter out = new PrintWriter(
                socket.getOutputStream(),
                true
            );
	    
	    BufferedReader in = new BufferedReader(
                new InputStreamReader(
                    socket.getInputStream()
                )
            );

            out.println("PUT " + key + " " + value);

	    String response = in.readLine();

            socket.close();

	    return response;

        } catch (IOException e) {

            return "ERROR";
        }
    }

    // =========================
    // IN RANGE
    // =========================
    /* Determines whether a given identifier belongs to the
     * circular interval (start, end) in the Chord ring.
     *
     * Since the identifier space is circular, two cases
     * must be considered:
     *
     * 1. Standard interval:
     *      start < end
     *
     * 2. Circular interval crossing zero:
     *      start > end
     *
     * Example:
     *      (215, 11)
     *
     * corresponds to:
     *
     *      (215, 255] U [0, 11)
     *
     * @param key Identifier being evaluated.
     * @param start Beginning of the interval (exclusive).
     * @param end End of the interval (exclusive).
     *
     * @return true if the identifier belongs to the interval;
     *         false otherwise.
     */
    boolean inRange(BigInteger key, BigInteger start, BigInteger end) {
        if (start.compareTo(end) < 0) {

            return key.compareTo(start) > 0 && key.compareTo(end) < 0;
        } else {

            return key.compareTo(start) > 0 || key.compareTo(end) < 0;
        }
    }

    // =========================
    // FIND SUCCESSOR
    // =========================
    /* Finds the node responsible for a given identifier.
     *
     * According to the Chord protocol, a node is responsible
     * for all identifiers belonging to the interval:
     *
     *      (predecessor, currentNode]
     *
     * If the current node is responsible for the key,
     * the search terminates locally.
     *
     * Otherwise, the request is forwarded to the closest
     * preceding node obtained from the finger table.
     *
     * This recursive lookup mechanism guarantees an
     * expected complexity of O(log n).
     *
     * @param key Identifier whose successor is being searched.
     *
     * @return Finger entry containing the responsible node.
     */
    Finger findSuccessor(BigInteger key) {
        if (isResponsible(key)) {

            return new Finger(key, id, port);
        }

	Finger next = closestPrecedingFinger(key);

	//System.out.println("[LOOKUP] " + id + " -> " + next.nodeId + " searching " + key);

        return sendFindSuccessor(next.port, key);
    }

    /* Sends a FINDSUCCESSOR request to another node.
     *
     * This method is part of the distributed lookup
     * algorithm implemented by Chord. The request is
     * forwarded until the responsible node is found.
     *
     * @param port Destination node port.
     * @param key Identifier being searched.
     *
     * @return Finger entry containing the successor node
     *         responsible for the identifier.
     */
    Finger sendFindSuccessor(int port, BigInteger key){

	try {

	    Socket socket =
                    new Socket("localhost", port);

            PrintWriter out = new PrintWriter(
                socket.getOutputStream(),
                true
            );
	    
	    BufferedReader in = new BufferedReader(
                new InputStreamReader(
                    socket.getInputStream()
                )
            );

            out.println("FINDSUCCESSOR " + key);

	    String response = in.readLine();

            socket.close();

	    String[] parts = response.split(",");

            return new Finger(new BigInteger(parts[0]), new BigInteger(parts[1]), Integer.parseInt(parts[2]));

	} catch (IOException e) {

            return null;
        }
    }

    // =========================
    // FINGER TABLE
    // ========================= 
    /* Builds the complete finger table for the current node.
     *
     * For each entry i, computes:
     *
     *      finger[i].start = (n + 2^i) mod 2^M
     *
     * and determines the successor node responsible for that
     * identifier in the Chord ring.
     *
     * Each finger table entry stores:
     *  - The start value of the interval.
     *  - The identifier of the successor node.
     *  - The port associated with that node.
     *
     * This method is usually executed when:
     *  - A node joins the network.
     *  - The routing information needs to be refreshed.
     */
    void buildFingerTable() {

        fingerTable.clear();

        for (int i = 0; i < M; i++) {

            BigInteger start = id.add(BigInteger.TWO.pow(i)).mod(ringSize);

            Finger successor = findSuccessor(start);

            fingerTable.add(new Finger(start, successor.nodeId, successor.port));
        }
    }

    /* Displays the contents of the current node's finger table.
     *
     * For each entry, the following information is shown:
     *  - Table index.
     *  - Start identifier of the interval.
     *  - Identifier of the successor node.
     *  - Port of the successor node.
     *
     * This method is mainly intended for debugging
     * and verification purposes.
     */
    void printFingerTable(){

	int index = 0;

	System.out.println("Finger table for node: " + this.id);
	
	for (Finger fingerEntry : fingerTable){
	    
	    System.out.println("i=" + index + " start=" + fingerEntry.start + " -> " + fingerEntry.nodeId + " : " + fingerEntry.port);

	    index ++;
	}
    }

    /* Updates the finger table of every node in the ring.
     *
     * The current node first rebuilds its own finger table
     * and then propagates an UPDATEFINGER message through
     * the ring so that all other nodes refresh their routing
     * information as well.
     *
     * This method is typically invoked after a new node joins
     * the Chord network.
     */
    void updateAllFingerTables() {
        
	buildFingerTable();

        if (this.successorPort != port) {

	    sendUpdateFinger(this.successorPort, this.id);
        }
    }

    /* Sends a request to another node instructing it to
     * rebuild its finger table.
     *
     * The sourceId parameter identifies the node that
     * originated the update operation, preventing the
     * update message from circulating indefinitely
     * around the ring.
     *
     * @param port Port of the node that will receive
     *             the update request.
     * @param sourceId Identifier of the node that
     *                 initiated the update process.
     */
    void sendUpdateFinger(int port, BigInteger sourceId) {

        try {

            Socket socket =
                new Socket("localhost", port);

            PrintWriter out =
                new PrintWriter(
                    socket.getOutputStream(),
                    true
            );

            out.println("UPDATEFINGER " + sourceId);

            socket.close();
        } catch (Exception e) {
        
	    e.printStackTrace();
        }
    }

    // =========================
    // PING
    // =========================
    /* Checks whether another node in the Chord ring is alive.
     *
     * A PING message is sent to the specified port and the
     * method waits for a PONG response. If the response is
     * received, the node is considered reachable.
     *
     * This mechanism is used by maintenance procedures such as
     * stabilize() and fixFingers() to detect failed nodes and
     * maintain routing consistency.
     *
     * @param port Port of the node to be checked.
     * @return true if the node responded with PONG;
     *         false otherwise.
     */
    boolean ping(int port) {

        try {

            Socket socket =
                new Socket("localhost", port);

            PrintWriter out =
                new PrintWriter(
                    socket.getOutputStream(),
                    true
            );

            BufferedReader in =
                new BufferedReader(
                    new InputStreamReader(
                        socket.getInputStream()
                    )
            );

            out.println("PING");

            String response = in.readLine();

            socket.close();

            return "PONG".equals(response);
        } catch (Exception e) {

            return false;
        }
    }

    // =========================
    // FIX FINGER
    // =========================
    /* Periodically refreshes a single entry of the finger table.
     *
     * Instead of rebuilding the entire finger table at once,
     * Chord updates one finger entry at a time. This approach
     * reduces maintenance overhead while gradually keeping
     * routing information consistent.
     *
     * The entry being refreshed is determined by the
     * fingerReviewed index, which advances cyclically
     * through all M entries.
     *
     * The update is only performed if the immediate successor
     * is reachable.
     */
    void fixFingers(){
	
	if(ping(this.successorPort)){

            BigInteger start =
                id.add(BigInteger.TWO.pow(fingerReviewed))
                    .mod(ringSize);

            Finger successor = findSuccessor(start);

            fingerTable.set(
                fingerReviewed,
                new Finger(
                    start,
                    successor.nodeId,
                    successor.port
                )
            );

	    fingerReviewed = (fingerReviewed + 1) % M;
	}
    }

    // =========================
    // NOTIFY
    // =========================
    /* Notifies a node that another node may become its
     * predecessor.
     *
     * This method is part of the Chord stabilization process.
     * If the current predecessor is no longer available, or if
     * the notifying node should replace it, the predecessor
     * information is updated accordingly.
     *
     * Otherwise, the notification request is recursively
     * forwarded through the predecessor chain until the
     * appropriate successor is found.
     *
     * @param givenPredecessorId Identifier of the predecessor
     *                           known by the requesting node.
     * @param givenId Identifier of the node performing the
     *                notification.
     * @param givenPort Listening port of the notifying node.
     *
     * @return The identifier and port of the node that should
     *         become the successor, or an error message if no
     *         suitable successor could be determined.
     */
    String notify(BigInteger givenPredecessorId, BigInteger givenId, int givenPort){

	if((this.predecessorId.compareTo(givenPredecessorId) == 0) || (!ping(this.predecessorPort))){
            
	    this.predecessorId = givenId;

	    this.predecessorPort = givenPort;

            return this.id + "," + this.port;
	} else if (ping(this.predecessorPort)){

	    try {

		Socket socket =
                    new Socket("localhost", this.predecessorPort);
 
                PrintWriter out = new PrintWriter(
                    socket.getOutputStream(),
                    true
                );
	    
	        BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                        socket.getInputStream()
                    )
                );

                out.println("FINDNEXTSUCCESSOR " + this.successorId + " " + this.id + " " + this.port);

	        String response = in.readLine();

                socket.close();
 
	        return response;
	    } catch (Exception e){

		return "Error";
            }
	}

	return "Error doesn't found successor";
    }

    // =========================
    // STABILIZE
    // =========================
    /* Periodically verifies that the current successor is still
     * reachable and replaces it if a failure is detected.
     *
     * The procedure first checks whether the immediate successor
     * responds to a ping request. If the successor fails to
     * respond several consecutive times, the node searches its
     * finger table for the next available candidate.
     *
     * Once a reachable candidate is found, a notification
     * procedure is executed to determine the most appropriate
     * successor and update local routing information.
     *
     * After a successful replacement, the finger table is
     * rebuilt in order to maintain routing consistency.
     *
     * This mechanism provides basic fault tolerance by allowing
     * the Chord ring to recover from node failures.
     */
    void stabilize(){

	if (!ping(this.successorPort)) {

	    if(this.pingNumber == 3){

		Finger newSuccessor = fingerTable.get(0);

		BigInteger idSearch = this.successorId;

		boolean findSucesorFinger = true;

		int index = 0;

                while(findSucesorFinger && (index < M)){

                    newSuccessor = fingerTable.get(index);

		    index++;

		    idSearch = newSuccessor.nodeId;

                    findSucesorFinger = ((newSuccessor.port != this.successorPort) || (!ping(newSuccessor.port)));
		}

		if ((newSuccessor.port != this.successorPort) && ping(newSuccessor.port)){

		    try {
			
		        Socket socket =
                            new Socket("localhost", newSuccessor.port);

                        PrintWriter out = new PrintWriter(
                            socket.getOutputStream(),
                            true
                        );
	    
	                BufferedReader in = new BufferedReader(
                            new InputStreamReader(
                                socket.getInputStream()
                            )
                        );

                        out.println("FINDNEXTSUCCESSOR " + this.successorId + " " + this.id + " " + this.port);

	                String response = in.readLine();

                        socket.close(); 

		        String[] parts = response.split(",");

		        this.successorId = 
			    new BigInteger(parts[0]);

                        this.successorPort =
                            Integer.parseInt(parts[1]);

                        buildFingerTable();
		    } catch (Exception e) {

                        System.out.println("Error in conection");
                    }
		} else {

		    System.out.println("Error doesn't exist next node");
		}

		this.pingNumber = 0;
	    } else{

	        this.pingNumber++;
	    }
	} else {

	    this.pingNumber = (this.pingNumber > 0) ? 0 : this.pingNumber;
	}
    }
    /* Executes periodic maintenance tasks required by
     * the Chord protocol.
     *
     * This method is invoked periodically by the scheduler
     * and performs:
     *
     * 1. Incremental finger table maintenance.
     * 2. Successor verification and stabilization.
     *
     * Maintenance tasks are only executed after the node
     * has successfully joined a ring.
     */
    void occassionallyReviews(){
	
	if(this.predecessorPort != 0){
	    fixFingers();

            stabilize();
	} 
    }
    
    // =========================
    // MAIN
    // =========================
    /* Entry point of the application.
     *
     * The method creates a Chord node, starts its server
     * thread, schedules periodic maintenance tasks and
     * provides a simple command-line interface for user
     * interaction.
     *
     * Supported commands:
     *
     * CONNECT <port>
     *      Connects the current node to an existing ring.
     *
     * PUT <key> <value>
     *      Stores a key-value pair in the distributed
     *      hash table.
     *
     * GET <key>
     *      Retrieves the value associated with a key.
     *
     * PRINTFINGERTABLE
     *      Displays the node's finger table.
     *
     * PRINTSUCCESSOR
     *      Displays the current successor identifier.
     *
     * PRINTPREDECESSOR
     *      Displays the current predecessor identifier.
     *
     * Program argument format:
     *
     *      java Node host:port
     *
     * Example:
     *
     *      java Node localhost:5001
     *
     * @param args Command-line arguments.
     */
    public static void main(String[] args) {

	String parser = args[0];
	String[] direccion = parser.split(":");
        String host = direccion[0];
        int port = Integer.parseInt(direccion[1]);

        Node node = new Node(host, port);

	System.out.println("[NODE] Port=" + port);

	System.out.println("[NODE] ID=" + host);

	scheduler.scheduleAtFixedRate(() -> node.occassionallyReviews(), 0, 5, TimeUnit.SECONDS);

        new Thread(() -> node.startServer()).start();

        Scanner scanner = new Scanner(System.in);

        while (true) {

            String line = scanner.nextLine();

            String[] parts = line.split(" ");

            if (parts[0].equals("CONNECT")) {

                int portToConnect =
                    Integer.parseInt(parts[1]);

                node.sendConnection(portToConnect);
            }

            else if (parts[0].equals("PUT")) {

                String key = parts[1]; 

                String value = parts[2];

                String response = node.put(key, value, false);

		System.out.println(response);
            }

            else if (parts[0].equals("GET")) {

                String key = parts[1];

                System.out.println(
                    "[RESULT] "
                    + node.get(key, true)
                );
            }

	    else if (parts[0].equals("PRINTFINGERTABLE")) {

                node.printFingerTable();
            }

            else if (parts[0].equals("PRINTSUCCESSOR")) {

                System.out.println(node.successorId.toString());
            }

	    else if (parts[0].equals("PRINTPREDECESSOR")) {

                System.out.println(node.predecessorId.toString());
            }
        }
    }
}
