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

    Finger findSuccessor(BigInteger key) {
        if (isResponsible(key)) {

            return new Finger(key, id, port);
        }

	Finger next = closestPrecedingFinger(key);

	//System.out.println("[LOOKUP] " + id + " -> " + next.nodeId + " searching " + key);

        return sendFindSuccessor(next.port, key);
    }

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

    void buildFingerTable() {

        fingerTable.clear();

        for (int i = 0; i < M; i++) {

            BigInteger start = id.add(BigInteger.TWO.pow(i)).mod(ringSize);

            Finger successor = findSuccessor(start);

            fingerTable.add(new Finger(start, successor.nodeId, successor.port));
        }
    }

    void printFingerTable(){

	int index = 0;

	System.out.println("Finger table for node: " + this.id);
	
	for (Finger fingerEntry : fingerTable){
	    
	    System.out.println("i=" + index + " start=" + fingerEntry.start + " -> " + fingerEntry.nodeId + " : " + fingerEntry.port);

	    index ++;
	}
    }

    void updateAllFingerTables() {
        
	buildFingerTable();

        if (this.successorPort != port) {

	    sendUpdateFinger(this.successorPort, this.id);
        }
    }

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

    void occassionallyReviews(){
	
	if(this.predecessorPort != 0){
	    fixFingers();

            stabilize();
	} 
    }
    
    // =========================
    // MAIN
    // =========================

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
