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
        
        System.out.println("[Node " + id + "] Starting election for generation " + currentGeneration + ", lastLogOffset=" + lastLogOffset);

        Message req = new Message();
        req.type = "REQUEST_VOTE";
        req.generation = currentGeneration;
        req.candidateId = id;
        req.lastLogOffset = lastLogOffset;
        req.lastLogGeneration = lastLogGen;

        // Collect votes. Including self, we have 1 vote.
        final int[] votes = {1};
        final int majority = (peers.size() + 1) / 2 + 1;
        System.out.println("[Node " + id + "] Majority needed: " + majority + ", current votes: " + votes[0]);

        if (votes[0] >= majority) {
            System.out.println("[Node " + id + "] Single node majority won!");
            transitionToLeader();
            return;
        }

        for (Map.Entry<Integer, String> peer : peers.entrySet()) {
            threadPool.execute(() -> {
                System.out.println("[Node " + id + "] Sending REQUEST_VOTE to Peer " + peer.getKey() + " (" + peer.getValue() + ")");
                Message resp = sendMessageToPeer(peer.getValue(), req);
                if (resp != null && "REQUEST_VOTE_RESPONSE".equals(resp.type)) {
                    synchronized (this) {
                        System.out.println("[Node " + id + "] Received REQUEST_VOTE_RESPONSE from Peer " + resp.nodeId + ", voteGranted=" + resp.voteGranted + ", generation=" + resp.generation);
                        if (role == Role.CANDIDATE && resp.generation == currentGeneration && resp.voteGranted) {
                            votes[0]++;
                            System.out.println("[Node " + id + "] Vote count: " + votes[0]);
                            if (votes[0] >= majority) {
                                transitionToLeader();
                            }
                        } else if (resp.generation > currentGeneration) {
                            System.out.println("[Node " + id + "] Found higher generation " + resp.generation + ", stepping down");
                            stepDown(resp.generation);
                        }
                    }
                } else {
                    System.out.println("[Node " + id + "] Failed to get REQUEST_VOTE_RESPONSE from Peer " + peer.getKey());
                }
            });
        }
    }

    private synchronized void transitionToLeader() {
        if (role != Role.CANDIDATE) return;
        role = Role.LEADER;
        System.out.println("[Node " + id + "] Transitioned to LEADER for generation " + currentGeneration);
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
        System.out.println("[Node " + id + "] Stepped down to FOLLOWER for generation " + currentGeneration);
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
                if (!entriesToSend.isEmpty()) {
                    System.out.println("[Node " + id + "] Sending APPEND_ENTRIES to Peer " + peer.getKey() + ", entries=" + entriesToSend.size() + ", prevOffset=" + prevOffset);
                }
                Message resp = sendMessageToPeer(peer.getValue(), req);
                if (resp != null && "APPEND_ENTRIES_RESPONSE".equals(resp.type)) {
                    synchronized (this) {
                        if (role == Role.LEADER && resp.generation == currentGeneration) {
                            if (resp.success) {
                                if (!entriesToSend.isEmpty()) {
                                    System.out.println("[Node " + id + "] Peer " + resp.nodeId + " successfully replicated up to matchOffset " + resp.matchOffset);
                                }
                                matchOffset.put(peer.getKey(), resp.matchOffset);
                                nextOffset.put(peer.getKey(), resp.matchOffset + 1);
                                checkAndUpdateCommitOffset();
                            } else {
                                System.out.println("[Node " + id + "] Peer " + peer.getKey() + " rejected APPEND_ENTRIES, decrementing nextOffset");
                                long currentNext = nextOffset.getOrDefault(peer.getKey(), 1L);
                                if (currentNext > 1) {
                                    nextOffset.put(peer.getKey(), currentNext - 1);
                                }
                            }
                        } else if (resp.generation > currentGeneration) {
                            System.out.println("[Node " + id + "] Peer " + peer.getKey() + " has higher gen " + resp.generation + ", stepping down");
                            stepDown(resp.generation);
                        }
                    }
                } else if (resp == null && !entriesToSend.isEmpty()) {
                    System.out.println("[Node " + id + "] Failed to get APPEND_ENTRIES_RESPONSE from Peer " + peer.getKey());
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
        } catch (IOException e) {
            // Silence connection exceptions since nodes start up at slightly different times, but log for visibility if needed
            // System.err.println("[Node " + id + "] Socket exception for peer " + address + ": " + e.getMessage());
        }
        return null;
    }

    public synchronized Message processMessage(Message msg) {
        if ("REQUEST_VOTE".equals(msg.type)) {
            Message resp = new Message();
            resp.type = "REQUEST_VOTE_RESPONSE";
            resp.nodeId = id;
            resp.voteGranted = false;

            if (msg.generation > currentGeneration) {
                stepDown(msg.generation);
            }
            resp.generation = currentGeneration;

            long lastLogOffset = log.isEmpty() ? 0 : log.get(log.size() - 1).offset;
            long lastLogGen = log.isEmpty() ? 0 : log.get(log.size() - 1).generation;

            boolean logOk = msg.lastLogGeneration > lastLogGen || 
                           (msg.lastLogGeneration == lastLogGen && msg.lastLogOffset >= lastLogOffset);

            System.out.println("[Node " + id + "] Received REQUEST_VOTE from candidate " + msg.candidateId + " for gen " + msg.generation + 
                               " (currentGen=" + currentGeneration + ", votedFor=" + votedFor + ", logOk=" + logOk + ")");

            if (msg.generation == currentGeneration && logOk && (votedFor == null || votedFor == msg.candidateId)) {
                resp.voteGranted = true;
                votedFor = msg.candidateId;
                System.out.println("[Node " + id + "] Voted for Candidate " + msg.candidateId + " for gen " + currentGeneration);
                resetElectionTimeout();
            }
            return resp;
        }

        if ("APPEND_ENTRIES".equals(msg.type)) {
            Message resp = new Message();
            resp.type = "APPEND_ENTRIES_RESPONSE";
            resp.nodeId = id;
            resp.success = false;

            if (msg.generation > currentGeneration) {
                stepDown(msg.generation);
            }
            resp.generation = currentGeneration;

            if (msg.generation == currentGeneration) {
                if (role != Role.FOLLOWER) {
                    role = Role.FOLLOWER;
                }
                resetElectionTimeout();
                votedFor = msg.leaderId;

                // Consistency check
                boolean hasPrev = msg.prevLogOffset == 0 || 
                                  (msg.prevLogOffset <= log.size() && log.get((int)msg.prevLogOffset - 1).generation == msg.prevLogGeneration);

                if (hasPrev) {
                    resp.success = true;
                    // Append and Truncate
                    long index = msg.prevLogOffset;
                    for (LogEntry newEntry : msg.entries) {
                        index++;
                        if (log.size() >= index) {
                            LogEntry existing = log.get((int)index - 1);
                            if (existing.generation != newEntry.generation) {
                                // Truncate from this index onwards
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

                    // Commit advancement
                    if (msg.leaderCommitOffset > commitOffset) {
                        commitOffset = Math.min(msg.leaderCommitOffset, resp.matchOffset);
                        applyCommitted();
                        this.notifyAll();
                    }
                }
            }
            return resp;
        }

        if ("GET_LOCAL".equals(msg.type)) {
            Message resp = new Message();
            resp.status = "VALUE";
            resp.value = stateMachine.get(msg.key);
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
                // SET operation
                System.out.println("[Node " + id + "] Received client SET request: key=" + msg.key + ", value=" + msg.value + ", logSize=" + log.size());
                LogEntry entry = new LogEntry(log.size() + 1, currentGeneration, "SET " + msg.key + " " + msg.value);
                appendEntry(entry);
                System.out.println("[Node " + id + "] Appended log entry offset=" + entry.offset + " for gen " + currentGeneration + ". Waiting for replication...");
                
                // Block until committed (majority replicate)
                boolean committed = waitForCommit(entry.offset, 2000);
                System.out.println("[Node " + id + "] Block finished, committed=" + committed + ", commitOffset=" + commitOffset);
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

    private synchronized boolean waitForCommit(long offset, long timeoutMs) {
        long start = System.currentTimeMillis();
        while (commitOffset < offset) {
            long elapsed = System.currentTimeMillis() - start;
            long remaining = timeoutMs - elapsed;
            if (remaining <= 0) {
                break;
            }
            try {
                this.wait(remaining);
            } catch (InterruptedException e) {
                break;
            }
        }
        return commitOffset >= offset;
    }

    private void checkAndUpdateCommitOffset() {
        // Find highest offset N that is replicated on majority
        long lastLogOffset = log.isEmpty() ? 0 : log.get(log.size() - 1).offset;
        for (long N = lastLogOffset; N > commitOffset; N--) {
            int count = 1; // Include leader
            for (long match : matchOffset.values()) {
                if (match >= N) {
                    count++;
                }
            }
            if (count >= (peers.size() + 1) / 2 + 1) {
                if (log.get((int)N - 1).generation == currentGeneration) {
                    System.out.println("[Node " + id + "] Advancing commitOffset up to " + N + " (majority match, gen matches current)");
                    commitOffset = N;
                    applyCommitted();
                    this.notifyAll(); // Wake up client threads waiting in waitForCommit
                    break;
                } else {
                    System.out.println("[Node " + id + "] Not advancing commitOffset to " + N + " because log entry generation " + log.get((int)N - 1).generation + " != currentGeneration " + currentGeneration);
                }
            }
        }
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
