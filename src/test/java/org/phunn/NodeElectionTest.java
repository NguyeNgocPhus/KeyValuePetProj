package org.phunn;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

public class NodeElectionTest {
    private Node node1, node2;

    @BeforeEach
    public void setUp() throws Exception {
        new File("node_1.log").delete();
        new File("node_1.log.meta").delete();
        new File("node_2.log").delete();
        new File("node_2.log.meta").delete();
        node1 = new Node(1, 19001, Map.of(2, "127.0.0.1:19002"), "node_1.log");
        node2 = new Node(2, 19002, Map.of(1, "127.0.0.1:19001"), "node_2.log");
    }

    @AfterEach
    public void tearDown() {
        node1.stop();
        node2.stop();
        new File("node_1.log").delete();
        new File("node_1.log.meta").delete();
        new File("node_2.log").delete();
        new File("node_2.log.meta").delete();
    }

    @Test
    public void testLeaderElection() throws Exception {
        node1.start();
        node2.start();
        
        // Wait for election timeout & vote exchange
        Thread.sleep(1200);
        
        // At least one should be leader, or both have advanced generations
        assertTrue(node1.currentGeneration > 0 || node2.currentGeneration > 0);
        assertTrue(node1.role == Node.Role.LEADER || node2.role == Node.Role.LEADER);
    }
}
