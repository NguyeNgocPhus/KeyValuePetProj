package org.phunn;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: java org.phunn.Main <id> <port> <walPath> <peerId1:host:port> [peerId2:host:port] ...");
            System.exit(1);
        }

        int id = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);
        String walPath = args[2];

        Map<Integer, String> peers = new HashMap<>();
        for (int i = 3; i < args.length; i++) {
            String[] parts = args[i].split(":", 2);
            int peerId = Integer.parseInt(parts[0]);
            String peerAddr = parts[1];
            peers.put(peerId, peerAddr);
        }

        System.out.println("Starting Node " + id + " on port " + port + "...");
        Node node = new Node(id, port, peers, walPath);
        node.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Node " + id + "...");
            node.stop();
        }));

        while (node.running) {
            Thread.sleep(1000);
        }
    }
}
