# Distributed Key-Value Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a distributed key-value store simulating Redis/etcd using raw TCP sockets, featuring a Generation Clock (Term), High-Water Mark (Commit Index/Offset), Replicated Log (WAL), and automated Leader Election with Log Truncation.

**Architecture:** A 3-node cluster communicating via TCP sockets using a line-by-line JSON protocol. A separate test orchestrator will spawn, kill, and restart the processes to test real failure and recovery behaviors.

**Tech Stack:** Java 25, Maven, Jackson Databind, JUnit 5.

---

### Task 1: Project Setup and Dependencies

**Files:**
- Modify: `pom.xml`

- [ ] **Step 1: Modify pom.xml to add Jackson and JUnit dependencies**

Open [pom.xml](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/pom.xml) and replace its contents to include Jackson Databind for JSON processing and JUnit 5 for testing.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.phunn</groupId>
    <artifactId>distributed_system</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>25</maven.compiler.source>
        <maven.compiler.target>25</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <jackson.version>2.18.2</jackson.version>
        <junit.version>5.11.4</junit.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <version>${jackson.version}</version>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter-api</artifactId>
            <version>${junit.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter-engine</artifactId>
            <version>${junit.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.13.0</version>
                <configuration>
                    <compilerArgs>
                        <arg>--enable-preview</arg>
                    </compilerArgs>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.5.2</version>
                <configuration>
                    <argLine>--enable-preview</argLine>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Run maven compile to verify dependencies**

Run: `mvn clean compile`
Expected: BUILD SUCCESS.

---

### Task 2: Models (`LogEntry` & `Message`)

**Files:**
- Create: `src/main/java/org/phunn/LogEntry.java`
- Create: `src/main/java/org/phunn/Message.java`
- Create: `src/test/java/org/phunn/MessageSerializationTest.java`

- [ ] **Step 1: Write a failing serialization test**

Create [MessageSerializationTest.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/test/java/org/phunn/MessageSerializationTest.java):
```java
package org.phunn;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class MessageSerializationTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testSerialization() throws Exception {
        Message msg = new Message();
        msg.type = "APPEND_ENTRIES";
        msg.generation = 1;
        LogEntry entry = new LogEntry(4, 1, "SET age 25");
        msg.entries = java.util.List.of(entry);

        String json = mapper.writeValueAsString(msg);
        assertTrue(json.contains("\"type\":\"APPEND_ENTRIES\""));
        assertTrue(json.contains("\"cmd\":\"SET age 25\""));

        Message deserialized = mapper.readValue(json, Message.class);
        assertEquals("APPEND_ENTRIES", deserialized.type);
        assertEquals(1, deserialized.generation);
        assertEquals(1, deserialized.entries.size());
        assertEquals(4, deserialized.entries.get(0).offset);
    }
}
```

- [ ] **Step 2: Run the test to verify compilation failure**

Run: `mvn test -Dtest=MessageSerializationTest`
Expected: Compile error because `Message` and `LogEntry` do not exist.

- [ ] **Step 3: Create LogEntry and Message classes**

Create [LogEntry.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/main/java/org/phunn/LogEntry.java):
```java
package org.phunn;

public class LogEntry {
    public long offset;
    public long generation;
    public String cmd;

    public LogEntry() {}

    public LogEntry(long offset, long generation, String cmd) {
        this.offset = offset;
        this.generation = generation;
        this.cmd = cmd;
    }
}
```

Create [Message.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/main/java/org/phunn/Message.java):
```java
package org.phunn;

import java.util.List;

public class Message {
    public String type;
    public long generation;
    public int leaderId;
    public int nodeId;
    public long prevLogOffset;
    public long prevLogGeneration;
    public List<LogEntry> entries;
    public long leaderCommitOffset;
    public boolean success;
    public long matchOffset;
    public int candidateId;
    public long lastLogOffset;
    public long lastLogGeneration;
    public boolean voteGranted;
    public String key;
    public String value;
    public String status;
    public String leaderAddress;
    public String message;

    public Message() {}
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn test -Dtest=MessageSerializationTest`
Expected: BUILD SUCCESS.

---

### Task 3: Write-Ahead Log Logger (`WalLogger`)

**Files:**
- Create: `src/main/java/org/phunn/WalLogger.java`
- Create: `src/test/java/org/phunn/WalLoggerTest.java`

- [ ] **Step 1: Write WAL unit tests for appending, loading, and truncating**

Create [WalLoggerTest.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/test/java/org/phunn/WalLoggerTest.java):
```java
package org.phunn;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.IOException;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class WalLoggerTest {
    private File tempFile;
    private WalLogger logger;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = File.createTempFile("wal_test", ".log");
        logger = new WalLogger(tempFile.getAbsolutePath());
    }

    @AfterEach
    public void tearDown() {
        if (tempFile.exists()) {
            tempFile.delete();
        }
    }

    @Test
    public void testAppendAndLoad() throws IOException {
        logger.append(new LogEntry(1, 1, "SET x 10"));
        logger.append(new LogEntry(2, 1, "SET y 20"));

        List<LogEntry> entries = logger.load();
        assertEquals(2, entries.size());
        assertEquals("SET x 10", entries.get(0).cmd);
        assertEquals(2, entries.get(1).offset);
    }

    @Test
    public void testTruncate() throws IOException {
        logger.append(new LogEntry(1, 1, "SET x 10"));
        logger.append(new LogEntry(2, 1, "SET y 20"));
        logger.append(new LogEntry(3, 2, "SET z 30"));

        // Truncate to offset 2 (delete offset >= 2)
        logger.truncate(2);

        List<LogEntry> entries = logger.load();
        assertEquals(1, entries.size());
        assertEquals("SET x 10", entries.get(0).cmd);
    }
}
```

- [ ] **Step 2: Run the test to verify compilation failure**

Run: `mvn test -Dtest=WalLoggerTest`
Expected: Compile error because `WalLogger` does not exist.

- [ ] **Step 3: Implement WalLogger**

Create [WalLogger.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/main/java/org/phunn/WalLogger.java):
```java
package org.phunn;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class WalLogger {
    private final String filepath;

    public WalLogger(String filepath) {
        this.filepath = filepath;
    }

    public synchronized void append(LogEntry entry) throws IOException {
        try (PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(filepath, true), StandardCharsets.UTF_8))) {
            out.println(entry.offset + " " + entry.generation + " " + entry.cmd);
            out.flush();
        }
    }

    public synchronized List<LogEntry> load() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        File file = new File(filepath);
        if (!file.exists()) {
            return entries;
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] parts = line.split(" ", 3);
                if (parts.length < 3) continue;
                long offset = Long.parseLong(parts[0]);
                long generation = Long.parseLong(parts[1]);
                String cmd = parts[2];
                entries.add(new LogEntry(offset, generation, cmd));
            }
        }
        return entries;
    }

    public synchronized void truncate(long fromOffset) throws IOException {
        List<LogEntry> entries = load();
        List<LogEntry> kept = new ArrayList<>();
        for (LogEntry e : entries) {
            if (e.offset < fromOffset) {
                kept.add(e);
            }
        }
        // Overwrite the WAL file with the kept logs
        try (PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(filepath, false), StandardCharsets.UTF_8))) {
            for (LogEntry e : kept) {
                out.println(e.offset + " " + e.generation + " " + e.cmd);
            }
            out.flush();
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn test -Dtest=WalLoggerTest`
Expected: BUILD SUCCESS.

---

### Task 4: Node Socket Core & Client Routing

**Files:**
- Create: `src/main/java/org/phunn/Node.java`
- Create: `src/test/java/org/phunn/NodeRoutingTest.java`

- [ ] **Step 1: Write a basic test for node networking & routing**

Create [NodeRoutingTest.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/test/java/org/phunn/NodeRoutingTest.java):
```java
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
        node = new Node(1, 18001, Map.of(2, "127.0.0.1:18002", 3, "127.0.0.1:18003"), "node_1_wal.log");
        node.start();
        Thread.sleep(100);
    }

    @AfterEach
    public void tearDown() {
        node.stop();
        new File("node_1_wal.log").delete();
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
```

- [ ] **Step 2: Run the test to verify compilation failure**

Run: `mvn test -Dtest=NodeRoutingTest`
Expected: Compile error because `Node` does not exist.

- [ ] **Step 3: Implement the Node skeleton**

Create [Node.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/main/java/org/phunn/Node.java):
```java
package org.phunn;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class Node {
    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    public final int id;
    public final int port;
    public final Map<Integer, String> peers; // ID -> "host:port"
    public final String walPath;
    
    public Role role = Role.FOLLOWER;
    public long currentGeneration = 0;
    public Integer votedFor = null;
    
    public final List<LogEntry> log = new ArrayList<>();
    public long commitOffset = 0;
    public long lastApplied = 0;
    public final ConcurrentHashMap<String, String> stateMachine = new ConcurrentHashMap<>();
    
    public final ObjectMapper mapper = new ObjectMapper();
    public ServerSocket serverSocket;
    public volatile boolean running = false;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    
    // Leader specific state
    public final Map<Integer, Long> nextOffset = new ConcurrentHashMap<>();
    public final Map<Integer, Long> matchOffset = new ConcurrentHashMap<>();

    public Node(int id, int port, Map<Integer, String> peers, String walPath) {
        this.id = id;
        this.port = port;
        this.peers = peers;
        this.walPath = walPath;
    }

    public synchronized void start() throws IOException {
        running = true;
        // Load log from WAL
        WalLogger wal = new WalLogger(walPath);
        this.log.addAll(wal.load());
        for (LogEntry e : this.log) {
            if (e.offset <= commitOffset) {
                applyToStateMachine(e);
            }
        }
        
        serverSocket = new ServerSocket(port);
        threadPool.execute(this::listen);
    }

    public synchronized void stop() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}
        threadPool.shutdownNow();
    }

    private void listen() {
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                threadPool.execute(() -> handleConnection(clientSocket));
            } catch (IOException e) {
                if (!running) break;
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true)) {
            String line;
            while (running && (line = in.readLine()) != null) {
                Message msg = mapper.readValue(line, Message.class);
                Message resp = processMessage(msg);
                if (resp != null) {
                    out.println(mapper.writeValueAsString(resp));
                }
            }
        } catch (IOException ignored) {}
    }

    public synchronized Message processMessage(Message msg) {
        if ("SET".equals(msg.type) || "GET".equals(msg.type)) {
            if (role != Role.LEADER) {
                Message redirect = new Message();
                redirect.status = "REDIRECT";
                if (votedFor != null && peers.containsKey(votedFor)) {
                    redirect.leaderAddress = peers.get(votedFor);
                } else {
                    redirect.leaderAddress = peers.values().stream().findFirst().orElse("");
                }
                return redirect;
            }
            if ("GET".equals(msg.type)) {
                Message resp = new Message();
                resp.status = "VALUE";
                resp.value = stateMachine.get(msg.key);
                return resp;
            } else {
                Message resp = new Message();
                resp.status = "OK";
                return resp;
            }
        }
        return null;
    }

    private void applyToStateMachine(LogEntry entry) {
        if (entry.cmd.startsWith("SET ")) {
            String[] parts = entry.cmd.split(" ", 3);
            if (parts.length == 3) {
                stateMachine.put(parts[1], parts[2]);
            }
        }
        lastApplied = entry.offset;
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn test -Dtest=NodeRoutingTest`
Expected: BUILD SUCCESS.

---

### Task 5: Leader Election & Heartbeats

**Files:**
- Modify: `src/main/java/org/phunn/Node.java`
- Create: `src/test/java/org/phunn/NodeElectionTest.java`

- [ ] **Step 1: Write election test to verify state transition on heartbeat failure**

Create [NodeElectionTest.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/test/java/org/phunn/NodeElectionTest.java):
```java
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
        new File("node_2.log").delete();
        node1 = new Node(1, 19001, Map.of(2, "127.0.0.1:19002"), "node_1.log");
        node2 = new Node(2, 19002, Map.of(1, "127.0.0.1:19001"), "node_2.log");
    }

    @AfterEach
    public void tearDown() {
        node1.stop();
        node2.stop();
        new File("node_1.log").delete();
        new File("node_2.log").delete();
    }

    @Test
    public void testLeaderElection() throws Exception {
        node1.start();
        node2.start();
        
        Thread.sleep(1000);
        
        assertTrue(node1.currentGeneration > 0 || node2.currentGeneration > 0);
        assertTrue(node1.role == Node.Role.LEADER || node2.role == Node.Role.LEADER);
    }
}
```

- [ ] **Step 2: Run the test to verify failure**

Run: `mvn test -Dtest=NodeElectionTest`
Expected: FAIL.

- [ ] **Step 3: Modify Node.java to implement Heartbeat and Election timers**

Modify [Node.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/main/java/org/phunn/Node.java) by replacing its state management fields, timer creation, and election message handling.

```java
<<<<
    public Node(int id, int port, Map<Integer, String> peers, String walPath) {
        this.id = id;
        this.port = port;
        this.peers = peers;
        this.walPath = walPath;
    }

    public synchronized void start() throws IOException {
        running = true;
        // Load log from WAL
        WalLogger wal = new WalLogger(walPath);
        this.log.addAll(wal.load());
        for (LogEntry e : this.log) {
            if (e.offset <= commitOffset) {
                applyToStateMachine(e);
            }
        }
        
        serverSocket = new ServerSocket(port);
        threadPool.execute(this::listen);
    }
====
    private long lastHeartbeatReceived = 0;
    private final Random random = new Random();
    private long electionTimeoutMs;

    public Node(int id, int port, Map<Integer, String> peers, String walPath) {
        this.id = id;
        this.port = port;
        this.peers = peers;
        this.walPath = walPath;
        resetElectionTimeout();
    }

    private void resetElectionTimeout() {
        this.electionTimeoutMs = 150 + random.nextInt(150); // 150ms - 300ms
        this.lastHeartbeatReceived = System.currentTimeMillis();
    }

    public synchronized void start() throws IOException {
        running = true;
        // Load log from WAL
        WalLogger wal = new WalLogger(walPath);
        this.log.addAll(wal.load());
        for (LogEntry e : this.log) {
            if (e.offset <= commitOffset) {
                applyToStateMachine(e);
            }
        }
        
        serverSocket = new ServerSocket(port);
        threadPool.execute(this::listen);
        threadPool.execute(this::runTimerLoop);
    }
>>>>
```

Also, add election and heartbeat sending methods to `Node.java`. Add these before `processMessage`:

```java
    private void runTimerLoop() {
        while (running) {
            try {
                Thread.sleep(20);
                synchronized (this) {
                    long now = System.currentTimeMillis();
                    if (role == Role.LEADER) {
                        sendHeartbeats();
                    } else if (now - lastHeartbeatReceived > electionTimeoutMs) {
                        startElection();
                    }
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private void startElection() {
        role = Role.CANDIDATE;
        currentGeneration++;
        votedFor = id;
        resetElectionTimeout();
        
        long lastLogOffset = log.isEmpty() ? 0 : log.get(log.size() - 1).offset;
        long lastLogGen = log.isEmpty() ? 0 : log.get(log.size() - 1).generation;
        
        Message req = new Message();
        req.type = "REQUEST_VOTE";
        req.generation = currentGeneration;
        req.candidateId = id;
        req.lastLogOffset = lastLogOffset;
        req.lastLogGeneration = lastLogGen;

        final int[] votes = {1};
        final int majority = (peers.size() + 1) / 2 + 1;

        if (votes[0] >= majority) {
            transitionToLeader();
            return;
        }

        for (Map.Entry<Integer, String> peer : peers.entrySet()) {
            threadPool.execute(() -> {
                Message resp = sendMessageToPeer(peer.getValue(), req);
                if (resp != null && "REQUEST_VOTE_RESPONSE".equals(resp.type)) {
                    synchronized (this) {
                        if (role == Role.CANDIDATE && resp.generation == currentGeneration && resp.voteGranted) {
                            votes[0]++;
                            if (votes[0] >= majority) {
                                transitionToLeader();
                            }
                        } else if (resp.generation > currentGeneration) {
                            stepDown(resp.generation);
                        }
                    }
                }
            });
        }
    }

    private synchronized void transitionToLeader() {
        if (role != Role.CANDIDATE) return;
        role = Role.LEADER;
        long lastLogOffset = log.isEmpty() ? 0 : log.get(log.size() - 1).offset;
        for (int peerId : peers.keySet()) {
            nextOffset.put(peerId, lastLogOffset + 1);
            matchOffset.put(peerId, 0L);
        }
        sendHeartbeats();
    }

    private synchronized void stepDown(long higherGeneration) {
        role = Role.FOLLOWER;
        currentGeneration = higherGeneration;
        votedFor = null;
        resetElectionTimeout();
    }

    private void sendHeartbeats() {
        long lastLogOffset = log.isEmpty() ? 0 : log.get(log.size() - 1).offset;
        long lastLogGen = log.isEmpty() ? 0 : log.get(log.size() - 1).generation;

        for (Map.Entry<Integer, String> peer : peers.entrySet()) {
            long prevOffset = nextOffset.getOrDefault(peer.getKey(), lastLogOffset + 1) - 1;
            long prevGen = 0;
            if (prevOffset > 0 && prevOffset <= log.size()) {
                prevGen = log.get((int)prevOffset - 1).generation;
            }
            
            List<LogEntry> entriesToSend = new ArrayList<>();
            if (log.size() > prevOffset) {
                entriesToSend.addAll(log.subList((int)prevOffset, log.size()));
            }

            Message req = new Message();
            req.type = "APPEND_ENTRIES";
            req.generation = currentGeneration;
            req.leaderId = id;
            req.prevLogOffset = prevOffset;
            req.prevLogGeneration = prevGen;
            req.entries = entriesToSend;
            req.leaderCommitOffset = commitOffset;

            threadPool.execute(() -> {
                Message resp = sendMessageToPeer(peer.getValue(), req);
                if (resp != null && "APPEND_ENTRIES_RESPONSE".equals(resp.type)) {
                    synchronized (this) {
                        if (role == Role.LEADER && resp.generation == currentGeneration) {
                            if (resp.success) {
                                matchOffset.put(peer.getKey(), resp.matchOffset);
                                nextOffset.put(peer.getKey(), resp.matchOffset + 1);
                                checkAndUpdateCommitOffset();
                            } else {
                                long currentNext = nextOffset.getOrDefault(peer.getKey(), 1L);
                                if (currentNext > 1) {
                                    nextOffset.put(peer.getKey(), currentNext - 1);
                                }
                            }
                        } else if (resp.generation > currentGeneration) {
                            stepDown(resp.generation);
                        }
                    }
                }
            });
        }
    }

    private Message sendMessageToPeer(String address, Message msg) {
        String[] parts = address.split(":");
        try (Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            socket.setSoTimeout(100);
            out.println(mapper.writeValueAsString(msg));
            String responseStr = in.readLine();
            if (responseStr != null) {
                return mapper.readValue(responseStr, Message.class);
            }
        } catch (IOException ignored) {}
        return null;
    }
```

Now replace the `processMessage` method implementation in `Node.java` to support processing remote network messages.

```java
<<<<
    public synchronized Message processMessage(Message msg) {
        if ("SET".equals(msg.type) || "GET".equals(msg.type)) {
            if (role != Role.LEADER) {
                Message redirect = new Message();
                redirect.status = "REDIRECT";
                if (votedFor != null && peers.containsKey(votedFor)) {
                    redirect.leaderAddress = peers.get(votedFor);
                } else {
                    redirect.leaderAddress = peers.values().stream().findFirst().orElse("");
                }
                return redirect;
            }
            if ("GET".equals(msg.type)) {
                Message resp = new Message();
                resp.status = "VALUE";
                resp.value = stateMachine.get(msg.key);
                return resp;
            } else {
                Message resp = new Message();
                resp.status = "OK";
                return resp;
            }
        }
        return null;
    }
====
    public synchronized Message processMessage(Message msg) {
        if ("REQUEST_VOTE".equals(msg.type)) {
            Message resp = new Message();
            resp.type = "REQUEST_VOTE_RESPONSE";
            resp.generation = currentGeneration;
            resp.voteGranted = false;

            if (msg.generation > currentGeneration) {
                stepDown(msg.generation);
            }

            long lastLogOffset = log.isEmpty() ? 0 : log.get(log.size() - 1).offset;
            long lastLogGen = log.isEmpty() ? 0 : log.get(log.size() - 1).generation;

            boolean logOk = msg.lastLogGeneration > lastLogGen || 
                           (msg.lastLogGeneration == lastLogGen && msg.lastLogOffset >= lastLogOffset);

            if (msg.generation == currentGeneration && logOk && (votedFor == null || votedFor == msg.candidateId)) {
                resp.voteGranted = true;
                votedFor = msg.candidateId;
                resetElectionTimeout();
            }
            return resp;
        }

        if ("APPEND_ENTRIES".equals(msg.type)) {
            Message resp = new Message();
            resp.type = "APPEND_ENTRIES_RESPONSE";
            resp.generation = currentGeneration;
            resp.success = false;

            if (msg.generation > currentGeneration) {
                stepDown(msg.generation);
            }

            if (msg.generation == currentGeneration) {
                if (role != Role.FOLLOWER) {
                    role = Role.FOLLOWER;
                }
                resetElectionTimeout();
                votedFor = msg.leaderId;

                boolean hasPrev = msg.prevLogOffset == 0 || 
                                  (msg.prevLogOffset <= log.size() && log.get((int)msg.prevLogOffset - 1).generation == msg.prevLogGeneration);

                if (hasPrev) {
                    resp.success = true;
                    long index = msg.prevLogOffset;
                    for (LogEntry newEntry : msg.entries) {
                        index++;
                        if (log.size() >= index) {
                            LogEntry existing = log.get((int)index - 1);
                            if (existing.generation != newEntry.generation) {
                                try {
                                    new WalLogger(walPath).truncate(index);
                                } catch (IOException ignored) {}
                                while (log.size() >= index) {
                                    log.remove(log.size() - 1);
                                }
                                appendEntry(newEntry);
                            }
                        } else {
                            appendEntry(newEntry);
                        }
                    }
                    
                    resp.matchOffset = log.isEmpty() ? 0 : log.get(log.size() - 1).offset;

                    if (msg.leaderCommitOffset > commitOffset) {
                        commitOffset = Math.min(msg.leaderCommitOffset, resp.matchOffset);
                        applyCommitted();
                    }
                }
            }
            return resp;
        }

        if ("SET".equals(msg.type) || "GET".equals(msg.type)) {
            if (role != Role.LEADER) {
                Message redirect = new Message();
                redirect.status = "REDIRECT";
                if (votedFor != null && peers.containsKey(votedFor)) {
                    redirect.leaderAddress = peers.get(votedFor);
                } else {
                    redirect.leaderAddress = peers.values().stream().findFirst().orElse("");
                }
                return redirect;
            }

            if ("GET".equals(msg.type)) {
                Message resp = new Message();
                resp.status = "VALUE";
                resp.value = stateMachine.get(msg.key);
                return resp;
            } else {
                LogEntry entry = new LogEntry(log.size() + 1, currentGeneration, "SET " + msg.key + " " + msg.value);
                appendEntry(entry);
                
                boolean committed = waitForCommit(entry.offset, 2000);
                Message resp = new Message();
                if (committed) {
                    resp.status = "OK";
                } else {
                    resp.status = "ERROR";
                    resp.message = "Replication timeout";
                }
                return resp;
            }
        }
        return null;
    }

    private void appendEntry(LogEntry entry) {
        log.add(entry);
        try {
            new WalLogger(walPath).append(entry);
        } catch (IOException ignored) {}
    }

    private void applyCommitted() {
        for (long i = lastApplied + 1; i <= commitOffset; i++) {
            if (i <= log.size()) {
                applyToStateMachine(log.get((int)i - 1));
            }
        }
    }

    private boolean waitForCommit(long offset, long timeoutMs) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            if (commitOffset >= offset) {
                return true;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                break;
            }
        }
        return commitOffset >= offset;
    }

    private void checkAndUpdateCommitOffset() {
        long lastLogOffset = log.isEmpty() ? 0 : log.get(log.size() - 1).offset;
        for (long N = lastLogOffset; N > commitOffset; N--) {
            int count = 1;
            for (long match : matchOffset.values()) {
                if (match >= N) {
                    count++;
                }
            }
            if (count >= (peers.size() + 1) / 2 + 1) {
                if (log.get((int)N - 1).generation == currentGeneration) {
                    commitOffset = N;
                    applyCommitted();
                    break;
                }
            }
        }
    }
>>>>
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn test -Dtest=NodeElectionTest`
Expected: BUILD SUCCESS.

---

### Task 6: Replication & High-Water Mark

**Files:**
- Create: `src/test/java/org/phunn/NodeReplicationTest.java`

- [ ] **Step 1: Write replication unit test with a mock follower**

Create [NodeReplicationTest.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/test/java/org/phunn/NodeReplicationTest.java):
```java
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
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

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
        executor.execute(() -> {
            try (Socket socket = mockFollowerSocket.accept();
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true)) {
                
                String line = in.readLine();
                if (line != null) {
                    Message req = mapper.readValue(line, Message.class);
                    if ("APPEND_ENTRIES".equals(req.type)) {
                        Message resp = new Message();
                        resp.type = "APPEND_ENTRIES_RESPONSE";
                        resp.generation = 1;
                        resp.success = true;
                        resp.matchOffset = req.entries.isEmpty() ? 0 : req.entries.get(0).offset;
                        out.println(mapper.writeValueAsString(resp));
                    }
                }
            } catch (IOException ignored) {}
        });

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
```

- [ ] **Step 2: Run test to verify replication passes**

Run: `mvn test -Dtest=NodeReplicationTest`
Expected: BUILD SUCCESS.

---

### Task 7: Node Executable Entrypoint

**Files:**
- Modify: `src/main/java/org/phunn/Main.java`

- [ ] **Step 1: Implement Main method to boot node from CLI arguments**

Replace contents of [Main.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/main/java/org/phunn/Main.java) to start a Node instance from command line args:
```java
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
```

- [ ] **Step 2: Compile package**

Run: `mvn clean package`
Expected: BUILD SUCCESS.

---

### Task 8: Extreme Failover & Log Truncation Integration Test

**Files:**
- Create: `src/test/java/org/phunn/ClusterTest.java`

- [ ] **Step 1: Write complete automated multi-process integration test**

Create [ClusterTest.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/test/java/org/phunn/ClusterTest.java):
```java
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
        
        command.addAll(Arrays.asList(peers.split(" ")));

        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
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
        Process p1 = startNode(1, 8001, "2:127.0.0.1:8002 3:127.0.0.1:8003");
        Process p2 = startNode(2, 8002, "1:127.0.0.1:8001 3:127.0.0.1:8003");
        Process p3 = startNode(3, 8003, "1:127.0.0.1:8001 2:127.0.0.1:8002");

        System.out.println("--- Waiting for leader election...");
        Thread.sleep(2000);

        System.out.println("--- Sending write SET x 100 to Node 1...");
        Message setMsg = new Message();
        setMsg.type = "SET";
        setMsg.key = "x";
        setMsg.value = "100";
        
        Message resp = sendCommand(8001, setMsg);
        
        if (resp != null && "REDIRECT".equals(resp.status)) {
            String leaderAddr = resp.leaderAddress;
            int leaderPort = Integer.parseInt(leaderAddr.split(":")[1]);
            System.out.println("--- Redirected to Leader at port " + leaderPort);
            resp = sendCommand(leaderPort, setMsg);
        }
        
        assertNotNull(resp);
        assertEquals("OK", resp.status);
        System.out.println("--- Write succeeded!");

        int leaderPort = findLeaderPort();
        assertTrue(leaderPort > 0, "No leader found in cluster!");
        System.out.println("--- Current Leader is on port: " + leaderPort);

        Message getMsg = new Message();
        getMsg.type = "GET";
        getMsg.key = "x";
        Message getResp = sendCommand(leaderPort, getMsg);
        assertEquals("VALUE", getResp.status);
        assertEquals("100", getResp.value);

        System.out.println("--- Killing Leader on port " + leaderPort);
        if (leaderPort == 8001) {
            p1.destroyForcibly();
        } else if (leaderPort == 8002) {
            p2.destroyForcibly();
        } else {
            p3.destroyForcibly();
        }

        System.out.println("--- Waiting for new leader election...");
        Thread.sleep(2500);

        int newLeaderPort = findLeaderPort();
        assertTrue(newLeaderPort > 0, "No new leader elected after failover!");
        assertNotEquals(leaderPort, newLeaderPort);
        System.out.println("--- New Leader is on port: " + newLeaderPort);

        System.out.println("--- Sending write SET x 200 to new leader...");
        setMsg.value = "200";
        resp = sendCommand(newLeaderPort, setMsg);
        assertNotNull(resp);
        assertEquals("OK", resp.status);

        System.out.println("--- Restarting old Leader...");
        if (leaderPort == 8001) {
            startNode(1, 8001, "2:127.0.0.1:8002 3:127.0.0.1:8003");
        } else if (leaderPort == 8002) {
            startNode(2, 8002, "1:127.0.0.1:8001 3:127.0.0.1:8003");
        } else {
            startNode(3, 8003, "1:127.0.0.1:8001 2:127.0.0.1:8002");
        }

        System.out.println("--- Waiting for old leader to sync with new leader...");
        Thread.sleep(3000);

        getResp = sendCommand(leaderPort, getMsg);
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
            
            socket.setSoTimeout(500);
            out.println(mapper.writeValueAsString(msg));
            String line = in.readLine();
            if (line != null) {
                return mapper.readValue(line, Message.class);
            }
        } catch (Exception ignored) {}
        return null;
    }
}
```

- [ ] **Step 2: Run integration tests to verify correctness**

Run: `mvn clean test`
Expected: BUILD SUCCESS.
