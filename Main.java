import java.util.*;

public class Main {

    public static void main(String[] args) {

        // Crear nodos
        Node n1 = new Node(1);
        Node n4 = new Node(4);
        Node n8 = new Node(8);
        Node n12 = new Node(12);

        // Armar anillo
        n1.successor = n4;
        n4.successor = n8;
        n8.successor = n12;
        n12.successor = n1;

        n1.predecessor = n12;
        n4.predecessor = n1;
        n8.predecessor = n4;
        n12.predecessor = n8;

        List<Node> nodes = Arrays.asList(n1, n4, n8, n12);

        // Construir finger tables simples
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
    }

    static void testRoute(Node start, int key) {
        System.out.println("\nBuscando key " + key + " desde nodo " + start.id);
        Node result = traceFindSuccessor(start, key);
        System.out.println("→ Sucesor final: " + result.id);
    }

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
}
