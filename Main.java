import java.util.*;

public class Main {

    public static void main(String[] args) {

        // Create nodes
        Node n1 = new Node(1);
        Node n4 = new Node(4);
        Node n8 = new Node(8);
        Node n12 = new Node(12);

        // Assemble the ring
        n1.successor = n4;
        n4.successor = n8;
        n8.successor = n12;
        n12.successor = n1;

        n1.predecessor = n12;
        n4.predecessor = n1;
        n8.predecessor = n4;
        n12.predecessor = n8;

        List<Node> nodes = Arrays.asList(n1, n4, n8, n12);

        // Build simple finger table
        for (Node n : nodes) {
            n.buildFingerTable(nodes);
        }

        System.out.println("Finger tables:");
        for (Node n : nodes) {
            System.out.print("Nodo " + n.id + ": ");
            for (Node f : n.fingerTable) {
                System.out.print(f.id + " ");
            }
            System.out.println();
        }

        System.out.println("\n=== Test findSuccessor con saltos ===");

        testRoute(n1, 10);
        testRoute(n4, 10);
        testRoute(n8, 10);
        testRoute(n12, 10);

	System.out.println("\n=== Test PUT / GET / CACHE / REPLICACIÓN ===");

        // Put test
        System.out.println("\nInsertando key=6, value='Hola'");
        n1.put(6, "Hola");

        // Node Status test
        System.out.println("\nEstado de los nodos:");
        printData(nodes);

        // Get from other node
        System.out.println("\nBuscando key=6 desde nodo 12:");
        String value = n12.get(6);
        System.out.println("Resultado: " + value);

        // Get from cache
        System.out.println("\nBuscando key=6 otra vez (cache):");
        value = n12.get(6);
        System.out.println("Resultado: " + value);

        // Wait cache error
        try {
            Thread.sleep(Node.CURRENT_TIME_FACTOR + 100);
        } catch (InterruptedException e) {}

        // Get after the cache expires
        System.out.println("\nBuscando key=6 después de expirar cache:");
        value = n12.get(6);
        System.out.println("Resultado: " + value);

        // Key not found
        System.out.println("\nBuscando key=20:");
        try {
            n4.get(20);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /*
     * Start a search for a key starting from a given node.
     * Display the step-by-step path and the final successor node.
     */
    static void testRoute(Node start, int key) {
        System.out.println("\nBuscando key " + key + " desde nodo " + start.id);
        Node result = traceFindSuccessor(start, key);
        System.out.println("→ Sucesor final: " + result.id);
    }

    /*
     * A recursive method that simulates the findSuccessor algorithm,
     * demonstrating how the query is routed between nodes in the ring.
     */
    static Node traceFindSuccessor(Node node, int key) {
        System.out.println("Nodo actual: " + node.id);

        if (node.inRange(key, node.id, node.successor.id)) {
            System.out.println("Encontrado entre " + node.id + " y " + node.successor.id);
            return node.successor;
        } else {
            Node next = node.closestPrecedingNode(key);
            System.out.println("Salto a: " + next.id);

            if (next == node) {
                return node.successor;
            }

            return traceFindSuccessor(next, key);
        }
    }

    /*
     * Displays the data stored on each node. 
     * It is used to verify key distribution and replication.
     */
    static void printData(List<Node> nodes) {
        for (Node n : nodes) {
            System.out.println("Nodo " + n.id + " almacena: " + n.data);
        }
    }
}
