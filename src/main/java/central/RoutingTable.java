/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/** @author alulo */

package central;

import java.util.*;

public class RoutingTable {

    public static class Node {
        public final String id;
        public final String host;
        public final int port;
        public Node(String id, String host, int port) { this.id = id; this.host = host; this.port = port; }
        @Override public String toString(){ return id + "@" + host + ":" + port; }
    }

    // parte -> [primary, replicaB, replicaC]
    private final Map<Integer, List<Node>> parts = new HashMap<>();

    public void putPart(int part, Node primary, Node replicaB, Node replicaC) {
        parts.put(part, Arrays.asList(primary, replicaB, replicaC));
    }

    public Node primaryOf(int part) { return parts.get(part).get(0); }
    public List<Node> replicasOf(int part) { return parts.get(part).subList(1, 3); }
    public List<Node> allOf(int part) { return parts.get(part); }
}