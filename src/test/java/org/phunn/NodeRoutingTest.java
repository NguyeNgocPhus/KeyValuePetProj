package org.phunn;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.net.Socket;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

public class NodeRoutingTest {
    private Node node;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    public void setUp() throws Exception {
        new File("node_1_wal.log").delete();
        new File("node_1_wal.log.meta").delete();
        node = new Node(1, 18001, Map.of(2, "127.0.0.1:18002", 3, "127.0.0.1:18003"), "node_1_wal.log");
        node.start();
        Thread.sleep(100);
    }

    @AfterEach
    public void tearDown() {
        node.stop();
        new File("node_1_wal.log").delete();
        new File("node_1_wal.log.meta").delete();
    }

    @Test
    public void testClientRequestRedirect() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", 18001);
             PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            Message clientReq = new Message();
            clientReq.type = "SET";
            clientReq.key = "username";
            clientReq.value = "alice";
            
            out.println(mapper.writeValueAsString(clientReq));
            String responseStr = in.readLine();
            assertNotNull(responseStr);
            
            Message resp = mapper.readValue(responseStr, Message.class);
            assertEquals("REDIRECT", resp.status);
        }
    }
}
