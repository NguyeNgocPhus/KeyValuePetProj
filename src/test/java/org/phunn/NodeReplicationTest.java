package org.phunn;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

public class NodeReplicationTest {
    private Node leader;
    private ServerSocket mockFollowerSocket;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @BeforeEach
    public void setUp() throws Exception {
        new File("leader.log").delete();
        mockFollowerSocket = new ServerSocket(19502);
        
        leader = new Node(1, 19501, Map.of(2, "127.0.0.1:19502"), "leader.log");
        leader.role = Node.Role.LEADER;
        leader.currentGeneration = 1;
        leader.nextOffset.put(2, 1L);
        leader.matchOffset.put(2, 0L);
        leader.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        leader.stop();
        mockFollowerSocket.close();
        executor.shutdownNow();
        new File("leader.log").delete();
    }

    @Test
    public void testSuccessfulReplication() throws Exception {
        // Start mock follower loop to handle incoming heartbeats and log replication requests
        executor.execute(() -> {
            while (!mockFollowerSocket.isClosed()) {
                try {
                    Socket socket = mockFollowerSocket.accept();
                    executor.execute(() -> {
                        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                             PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true)) {
                            String line;
                            while ((line = in.readLine()) != null) {
                                Message req = mapper.readValue(line, Message.class);
                                if ("APPEND_ENTRIES".equals(req.type)) {
                                    Message resp = new Message();
                                    resp.type = "APPEND_ENTRIES_RESPONSE";
                                    resp.generation = 1;
                                    resp.nodeId = 2;
                                    resp.success = true;
                                    
                                    long match = req.prevLogOffset;
                                    if (req.entries != null && !req.entries.isEmpty()) {
                                        match = req.entries.get(req.entries.size() - 1).offset;
                                    }
                                    resp.matchOffset = match;
                                    out.println(mapper.writeValueAsString(resp));
                                }
                            }
                        } catch (IOException ignored) {}
                    });
                } catch (IOException e) {
                    break;
                }
            }
        });

        // Send a SET request to the leader
        try (Socket clientSocket = new Socket("127.0.0.1", 19501);
             PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
            
            Message setReq = new Message();
            setReq.type = "SET";
            setReq.key = "fruit";
            setReq.value = "apple";
            
            out.println(mapper.writeValueAsString(setReq));
            String line = in.readLine();
            assertNotNull(line);
            
            Message resp = mapper.readValue(line, Message.class);
            assertEquals("OK", resp.status);
            assertEquals("apple", leader.stateMachine.get("fruit"));
        }
    }
}
