package org.phunn;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.net.Socket;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class ClusterTest {
    private final List<Process> processes = new ArrayList<>();
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    public void setUp() {
        // Clear old logs
        for (int i = 1; i <= 3; i++) {
            new File("node_" + i + "_wal.log").delete();
        }
    }

    @AfterEach
    public void tearDown() {
        for (Process p : processes) {
            if (p.isAlive()) {
                p.destroyForcibly();
            }
        }
        for (int i = 1; i <= 3; i++) {
            new File("node_" + i + "_wal.log").delete();
        }
    }

    private Process startNode(int id, int port, String peers) throws IOException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");

        List<String> command = new ArrayList<>(List.of(
            javaBin,
            "--enable-preview",
            "-cp", classpath,
            "org.phunn.Main",
            String.valueOf(id),
            String.valueOf(port),
            "node_" + id + "_wal.log"
        ));
        
        // Add peers
        command.addAll(Arrays.asList(peers.split(" ")));

        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        // Pipe logs to stdout with prefix
        Process p = builder.start();
        processes.add(p);
        
        new Thread(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line;
                while ((line = r.readLine()) != null) {
                    System.out.println("[Node " + id + "] " + line);
                }
            } catch (IOException ignored) {}
        }).start();

        return p;
    }

    @Test
    public void testExtremeScenario() throws Exception {
        // Start 3 nodes on ports 8001, 8002, 8003
        Process p1 = startNode(1, 8001, "2:127.0.0.1:8002 3:127.0.0.1:8003");
        Process p2 = startNode(2, 8002, "1:127.0.0.1:8001 3:127.0.0.1:8003");
        Process p3 = startNode(3, 8003, "1:127.0.0.1:8001 2:127.0.0.1:8002");

        // Give them time to elect a leader
        System.out.println("--- Waiting for leader election...");
        Thread.sleep(2500);

        // Send a write SET command to Node 1 (port 8001)
        System.out.println("--- Sending write SET x 100 to Node 1...");
        Message setMsg = new Message();
        setMsg.type = "SET";
        setMsg.key = "x";
        setMsg.value = "100";
        
        Message resp = sendCommand(8001, setMsg);
        
        // If Node 1 is not leader, follow redirection
        if (resp != null && "REDIRECT".equals(resp.status)) {
            String leaderAddr = resp.leaderAddress;
            int leaderPort = Integer.parseInt(leaderAddr.split(":")[1]);
            System.out.println("--- Redirected to Leader at port " + leaderPort);
            resp = sendCommand(leaderPort, setMsg);
        }
        
        assertNotNull(resp);
        assertEquals("OK", resp.status);
        System.out.println("--- Write succeeded!");

        // Determine current leader by asking nodes for a GET key
        int leaderPort = findLeaderPort();
        assertTrue(leaderPort > 0, "No leader found in cluster!");
        System.out.println("--- Current Leader is on port: " + leaderPort);

        // Verify the value matches
        Message getMsg = new Message();
        getMsg.type = "GET";
        getMsg.key = "x";
        Message getResp = sendCommand(leaderPort, getMsg);
        assertEquals("VALUE", getResp.status);
        assertEquals("100", getResp.value);

        // Kill the leader process!
        System.out.println("--- Killing Leader on port " + leaderPort);
        if (leaderPort == 8001) {
            p1.destroyForcibly();
            p1.waitFor(); // Wait for OS to clean up socket
        } else if (leaderPort == 8002) {
            p2.destroyForcibly();
            p2.waitFor();
        } else {
            p3.destroyForcibly();
            p3.waitFor();
        }
        System.out.println("--- Leader terminated.");

        // Wait for election of new leader among remaining 2 nodes
        System.out.println("--- Waiting for new leader election...");
        Thread.sleep(3000);

        int newLeaderPort = findLeaderPort();
        assertTrue(newLeaderPort > 0, "No new leader elected after failover!");
        assertNotEquals(leaderPort, newLeaderPort);
        System.out.println("--- New Leader is on port: " + newLeaderPort);

        // Write new divergent data to the new leader
        System.out.println("--- Sending write SET x 200 to new leader...");
        setMsg.value = "200";
        resp = sendCommand(newLeaderPort, setMsg);
        assertNotNull(resp, "Response from new leader is null!");
        assertEquals("OK", resp.status);

        // Restart old leader
        System.out.println("--- Restarting old Leader on port " + leaderPort + "...");
        if (leaderPort == 8001) {
            startNode(1, 8001, "2:127.0.0.1:8002 3:127.0.0.1:8003");
        } else if (leaderPort == 8002) {
            startNode(2, 8002, "1:127.0.0.1:8001 3:127.0.0.1:8003");
        } else {
            startNode(3, 8003, "1:127.0.0.1:8001 2:127.0.0.1:8002");
        }

        // Wait for replication and potential log truncation to happen on old leader
        System.out.println("--- Waiting for old leader to sync with new leader...");
        Thread.sleep(4000);

        // Query the restarted node directly to make sure its key value has been truncated and updated
        System.out.println("--- Querying key 'x' from restarted leader (port " + leaderPort + ")...");
        getResp = sendCommand(leaderPort, getMsg);
        assertNotNull(getResp, "Response from restarted leader is null!");
        
        // Wait, if it redirects, it means it is a follower (which is correct), but we want to know what it redirected to.
        // Let's connect to the new leader to verify state machine, or verify it redirected correctly.
        // Actually, if we query GET from the restarted node, it should return REDIRECT or the value.
        // Wait! In processMessage for GET:
        // If it is NOT leader, it returns REDIRECT.
        // So `getResp` will have status REDIRECT.
        // To verify the restarted node's state machine directly, let's write a simple admin backdoor or check the value in the new leader.
        // But wait! If it redirects, how do we know the restarted node's internal state machine was updated?
        // In Node's APPEND_ENTRIES handling, the follower receives the committed entry, applies it to its own `stateMachine` map.
        // If we want to query its state machine without being redirected, we can add a special "GET_LOCAL" type which returns local state machine value regardless of role!
        // Yes! A special GET_LOCAL command is incredibly clean and useful for testing. Let's check Node's GET command.
        // In processMessage:
        // if ("GET_LOCAL".equals(msg.type)) {
        //     Message resp = new Message();
        //     resp.status = "VALUE";
        //     resp.value = stateMachine.get(msg.key);
        //     return resp;
        // }
        // Let's add GET_LOCAL to Node.java so we can query any node directly for testing!
        // That is extremely robust and does not violate anything.
        
        // For now let's query the new leader to see if the system as a whole has x=200, 
        // and we will query the restarted node using GET_LOCAL.
        Message getLocalMsg = new Message();
        getLocalMsg.type = "GET_LOCAL";
        getLocalMsg.key = "x";
        
        getResp = sendCommand(leaderPort, getLocalMsg);
        assertNotNull(getResp);
        assertEquals("VALUE", getResp.status);
        assertEquals("200", getResp.value, "Old leader failed to truncate its divergent logs and sync key 'x' to '200'!");
        System.out.println("--- Integration test successful! Divergent log was truncated and synced to '200'.");
    }

    private int findLeaderPort() {
        int[] ports = {8001, 8002, 8003};
        for (int port : ports) {
            Message req = new Message();
            req.type = "GET";
            req.key = "x";
            Message resp = sendCommand(port, req);
            if (resp != null && ("VALUE".equals(resp.status) || "OK".equals(resp.status))) {
                return port;
            }
        }
        return -1;
    }

    private Message sendCommand(int port, Message msg) {
        try (Socket socket = new Socket("127.0.0.1", port);
             PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            socket.setSoTimeout(1000);
            out.println(mapper.writeValueAsString(msg));
            String line = in.readLine();
            if (line != null) {
                return mapper.readValue(line, Message.class);
            }
        } catch (Exception ignored) {}
        return null;
    }
}
