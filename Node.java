import java.io.*;
import java.net.*;
import java.util.*;

public class Node {

    static final String ERROR_KEY =
        "Error: The key %d doesn't have value";

    static final String WARNING_CACHE =
        "CACHE Entry for key %d expired and was removed.";

    static final int CURRENT_TIME_FACTOR = 5000;

    int id;
    int port;

    int successorPort;
    int successorId;
    int predecessorPort;
    int predecessorId;

    Map<Integer, String> data;
    Map<Integer, CacheEntry> cache;

    List<Finger> fingerTable;

    public Node(int id, int port) {

        this.id = id;
        this.port = port;

        this.successorPort = 0;
	this.successorId = 0;
        this.predecessorPort = 0;
        this.predecessorId = 0;

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

		int receivedId =
                    Integer.parseInt(parts[2]);

                String response =
                    getConnection(receivedPort, receivedId);

                out.println(response);
            }

	    // GETCHANGECONNECTION
            if (parts[0].equals("GETCHANGECONNECTION")) {

                int receivedPort =
                    Integer.parseInt(parts[1]);

		int receivedId =
                    Integer.parseInt(parts[2]);

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

                int key = Integer.parseInt(parts[1]);

                String value = parts[2];

		String response = put(key, value);

                out.println(response);
            }

            // GET
            else if (parts[0].equals("GET")) {

                int key = Integer.parseInt(parts[1]);

                String value = get(key, false);

                out.println(value);
            }

	    //PUTREPLICATION
	    else if (parts[0].equals("PUTREPLICATION")){
		int key = Integer.parseInt(parts[1]);
		
		String value = parts[2];

                data.put(key, value);
	    }

            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // =========================
    // CONNECTION
    // =========================

    public String getConnection(int portReceived, int idReceived) {

        if (this.successorPort == 0) {

            this.successorPort = portReceived;

	    this.successorId = idReceived;

            this.predecessorPort = portReceived;

            this.predecessorId = idReceived;

            System.out.println(
                "[NODE " + id + "] Connected with "
                + successorPort
            );

            return 0 + "," + this.port + "," + this.id;

        } else {

	    String value = sendChangeConnection(portReceived, idReceived);

	    return value;

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
                        Integer.parseInt(parts[2]);

                    this.predecessorPort =
                        Integer.parseInt(parts[1]);

                    this.predecessorId =
                        Integer.parseInt(parts[2]);

		    System.out.println(
                        "[NODE " + id + "] Connected with "
                            + predecessorPort
                    );
	        } else {
		
		    this.successorPort =
                        Integer.parseInt(parts[3]);

                    this.successorId =
                        Integer.parseInt(parts[4]);

                    this.predecessorPort =
                        Integer.parseInt(parts[1]);

                    this.predecessorId =
                        Integer.parseInt(parts[2]);

		    socket =
                        new Socket("localhost", successorPort);

                    out = new PrintWriter(
                        socket.getOutputStream(),
                        true
                    );

		    out.println("CHANGEKEYVALUE");

                    socket.close();

                    socket =
                        new Socket("localhost", predecessorPort);

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

    public String getChangeConnection(int portReceived, int idReceived) {

        if (this.id > idReceived) {

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

    public String sendChangeConnection(int port, int idReceived) {

        try {
	    boolean isSuccessor = this.id < idReceived && this.successorId > idReceived;

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
	Map<Integer, String> dumpData = new HashMap<>(data);
	data = new HashMap<>();
        for (Map.Entry<Integer, String> entry : dumpData.entrySet()) {
		put(entry.getKey(), entry.getValue());
	}
    }

    // =========================
    // PUT
    // =========================

    public String put(int key, String value) {

        if (isResponsible(key)) {

            data.put(key, value);

	    if (successorPort != 0 && successorPort != this.port){
                
		try {
		    Socket socket =
                        new Socket("localhost", successorPort);

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

            return sendPutNext(key, value);
        }
    }

    // =========================
    // GET
    // =========================

    public String get(int key, boolean isLocal) {
        // CACHE
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

            String value =
                sendGetFinger(next.port, key);

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

    boolean isResponsible(int key) {

        if (predecessorPort == 0) {
            return true;
        }

        if (predecessorId < id) {

            return key > predecessorId
                && key <= id;

        } else {

            return key > predecessorId
                || key <= id;
        }
    }

    // =========================
    // FORWARD GET
    // =========================

    String sendGetFinger(int port, int key) {

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

    Finger closestPrecedingFinger(int key) {

        if (fingerTable.isEmpty()) {
            return new Finger(successorId, successorPort);
        }

        for (int i = fingerTable.size() - 1; i >= 0; i--) {

            Finger finger = fingerTable.get(i);

            if (inRange(finger.id, this.id, key)) {
                return finger;
            }
        }

        return new Finger(successorId, successorPort);
    }

    // =========================
    // FORWARD PUT
    // =========================

    String sendPutNext(int key, String value) {

        try {

            Socket socket =
                new Socket("localhost", successorPort);

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

    boolean inRange(int key, int start, int end) {
        if (start < end) {

            return key > start && key <= end;
        } else {

            return key > start || key <= end;
        }
    }

    // =========================
    // MAIN
    // =========================

    public static void main(String[] args) {

        int id = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);

        Node node = new Node(id, port);

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

                int key = Integer.parseInt(parts[1]);

                String value = parts[2];

                String response = node.put(key, value);

		System.out.println(response);
            }

            else if (parts[0].equals("GET")) {

                int key = Integer.parseInt(parts[1]);

                System.out.println(
                    "[RESULT] "
                    + node.get(key, true)
                );
            }

	    else if (parts[0].equals("ADDFINGER")) {

                int fingerId =
                    Integer.parseInt(parts[1]);

                int fingerPort =
                    Integer.parseInt(parts[2]);

                node.fingerTable.add(
                    new Finger(fingerId, fingerPort)
                );

                System.out.println(
                    "[NODE " + node.id
                    + "] Finger added: "
                    + fingerId
                );
            }
        }
    }
}
