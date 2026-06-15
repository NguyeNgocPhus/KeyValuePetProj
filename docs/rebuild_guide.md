# Hướng dẫn lập trình từng bước (Feature-by-Feature): Tự xây dựng Distributed Key-Value Store

Tài liệu này hướng dẫn bạn xây dựng hệ thống phân tán một cách lũy tiến. Chúng ta sẽ bắt đầu từ một Server Socket đơn giản, sau đó thêm dần từng tính năng (Bầu cử, WAL, Replication, và cuối cùng là Truncation).

Sau mỗi tính năng, bạn có thể chạy thử để kiểm tra ngay lập tức.

---

## Lộ trình xây dựng 5 tính năng:
1.  **Feature 1**: Mạng kết nối Socket cơ bản & Map bộ nhớ cục bộ.
2.  **Feature 2**: Cơ chế bầu cử (Leader Election) & Heartbeat.
3.  **Feature 3**: Ghi đĩa bền vững (Write-Ahead Log - WAL).
4.  **Feature 4**: Nhân bản Log & Vạch nước cao (High-Water Mark / Commit Index).
5.  **Feature 5**: Cắt log lệch (Log Truncation) khi Leader cũ sống lại.

---

## 🛠️ Chuẩn bị dự án (`pom.xml`)
Đảm bảo file `pom.xml` ở thư mục gốc của bạn đã khai báo thư viện Jackson và kích hoạt tính năng compiler Java 25 preview:
```xml
<!-- Xem pom.xml hiện tại của dự án để biết cấu hình chi tiết -->
```

---

## Feature 1: Mạng kết nối Socket & Map bộ nhớ cục bộ
**Mục tiêu**: Cho các nút khởi động trên các cổng TCP khác nhau, chấp nhận kết nối từ Client, và cho phép đọc/ghi dữ liệu tạm thời trong bộ nhớ (`ConcurrentHashMap`).

### Bước 1.1: Tạo các Model truyền thông tin (`LogEntry.java` & `Message.java`)
Tạo cấu trúc dữ liệu cơ bản để đóng gói thông điệp gửi qua mạng dưới dạng JSON.

#### File: `src/main/java/org/phunn/LogEntry.java`
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

#### File: `src/main/java/org/phunn/Message.java`
```java
package org.phunn;

import java.util.List;

public class Message {
    public String type; // SET, GET, GET_LOCAL, REDIRECT, OK, VALUE
    public String key;
    public String value;
    public String status;
    public String leaderAddress;
    public int nodeId;
    
    // Các trường khác phục vụ thuật toán đồng thuận (sẽ dùng ở các bước sau)
    public long generation;
    public int candidateId;
    public long lastLogOffset;
    public long lastLogGeneration;
    public boolean voteGranted;
    public int leaderId;
    public long prevLogOffset;
    public long prevLogGeneration;
    public List<LogEntry> entries;
    public long leaderCommitOffset;
    public boolean success;
    public long matchOffset;

    public Message() {}
}
```

### Bước 1.2: Viết khung sườn Server Socket (`Node.java` - Phiên bản 1)
Viết lớp `Node` chỉ chứa Server Socket lắng nghe kết nối và xử lý yêu cầu Client đọc/ghi dữ liệu vào Map bộ nhớ `stateMachine`.

#### File: `src/main/java/org/phunn/Node.java`
```java
package org.phunn;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class Node {
    public final int id;
    public final int port;
    public final Map<Integer, String> peers;
    public final String walPath;
    
    public final ConcurrentHashMap<String, String> stateMachine = new ConcurrentHashMap<>();
    public final ObjectMapper mapper = new ObjectMapper();
    public ServerSocket serverSocket;
    public volatile boolean running = false;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();

    public Node(int id, int port, Map<Integer, String> peers, String walPath) {
        this.id = id;
        this.port = port;
        this.peers = peers;
        this.walPath = walPath;
    }

    public synchronized void start() throws IOException {
        running = true;
        serverSocket = new ServerSocket(port);
        threadPool.execute(this::listen);
        System.out.println("Node " + id + " started on port " + port);
    }

    public synchronized void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
        threadPool.shutdownNow();
    }

    private void listen() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                threadPool.execute(() -> handleConnection(socket));
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
        Message resp = new Message();
        if ("SET".equals(msg.type)) {
            // Tạm thời lưu vào bộ nhớ cục bộ
            stateMachine.put(msg.key, msg.value);
            resp.status = "OK";
            return resp;
        } else if ("GET".equals(msg.type) || "GET_LOCAL".equals(msg.type)) {
            resp.status = "VALUE";
            resp.value = stateMachine.get(msg.key);
            return resp;
        }
        return null;
    }
}
```

### 🔍 Cách chạy thử nghiệm Feature 1:
1.  Biên dịch dự án: `mvn clean compile`
2.  Mở terminal chạy thủ công Node 1:
    `java --enable-preview -cp target/classes org.phunn.Main 1 8001 node_1_wal.log 2:127.0.0.1:8002`
3.  Mở terminal khác, dùng netcat gửi gói tin SET:
    `echo '{"type":"SET","key":"test","value":"hello"}' | nc localhost 8001`
    *Kết quả trả về*: `{"status":"OK"}`
4.  Gửi gói tin GET:
    `echo '{"type":"GET","key":"test"}' | nc localhost 8001`
    *Kết quả trả về*: `{"status":"VALUE","value":"hello"}`

---

## Feature 2: Cơ chế bầu cử (Leader Election) & Heartbeat
**Mục tiêu**: Khi khởi động, các nút ở trạng thái FOLLOWER. Nếu quá hạn ngẫu nhiên (150ms-300ms) không nhận được Heartbeat, chúng chuyển sang CANDIDATE, tăng Generation thêm 1, đi xin phiếu bầu. Nếu được $\ge 2$ phiếu (quá bán trong cụm 3 nút), nút đó lên làm LEADER và gửi Heartbeat định kỳ (50ms).

### Bước 2.1: Thêm các biến trạng thái và Timers vào `Node.java`
Mở rộng file [Node.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/main/java/org/phunn/Node.java):
*   Thêm `role`, `currentGeneration`, `votedFor`.
*   Thêm `runTimerLoop()` kiểm tra quá hạn nhận heartbeat.
*   Thêm `startElection()` phát gói tin `REQUEST_VOTE` tới peers.

Thêm đoạn mã sau vào lớp `Node`:

```java
    public enum Role { FOLLOWER, CANDIDATE, LEADER }
    public Role role = Role.FOLLOWER;
    public long currentGeneration = 0;
    public Integer votedFor = null;

    private long lastHeartbeatReceived = 0;
    private final Random random = new Random();
    private long electionTimeoutMs;

    // Trong constructor Node:
    // resetElectionTimeout();

    private void resetElectionTimeout() {
        this.electionTimeoutMs = 150 + random.nextInt(150); // 150ms - 300ms
        this.lastHeartbeatReceived = System.currentTimeMillis();
    }

    // Cập nhật hàm start() để chạy timer:
    // threadPool.execute(this::runTimerLoop);

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
```

### Bước 2.2: Triển khai gửi nhận Vote và Heartbeat
Viết phương thức gửi tin nhắn đi và xử lý tin nhắn đến.

```java
    private void startElection() {
        role = Role.CANDIDATE;
        currentGeneration++;
        votedFor = id;
        resetElectionTimeout();
        System.out.println("[Node " + id + "] Starting election for gen " + currentGeneration);

        Message req = new Message();
        req.type = "REQUEST_VOTE";
        req.generation = currentGeneration;
        req.candidateId = id;

        final int[] votes = {1}; // Đã tự bầu cho mình
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
        System.out.println("[Node " + id + "] Transitioned to LEADER for gen " + currentGeneration);
        sendHeartbeats();
    }

    private synchronized void stepDown(long higherGeneration) {
        role = Role.FOLLOWER;
        currentGeneration = higherGeneration;
        votedFor = null;
        System.out.println("[Node " + id + "] Stepped down to FOLLOWER for gen " + currentGeneration);
        resetElectionTimeout();
    }

    private void sendHeartbeats() {
        for (Map.Entry<Integer, String> peer : peers.entrySet()) {
            Message req = new Message();
            req.type = "APPEND_ENTRIES";
            req.generation = currentGeneration;
            req.leaderId = id;
            // Dòng log trống đóng vai trò là heartbeat

            threadPool.execute(() -> {
                Message resp = sendMessageToPeer(peer.getValue(), req);
                if (resp != null && "APPEND_ENTRIES_RESPONSE".equals(resp.type)) {
                    synchronized (this) {
                        if (resp.generation > currentGeneration) {
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

Cập nhật `processMessage` của `Node.java` để xử lý gói tin bầu cử và Heartbeat:
```java
    // Thay thế processMessage cũ bằng logic này:
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

            // Bỏ phiếu nếu cùng kỳ và chưa bầu cho ai
            if (msg.generation == currentGeneration && (votedFor == null || votedFor == msg.candidateId)) {
                resp.voteGranted = true;
                votedFor = msg.candidateId;
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
                if (role != Role.FOLLOWER) role = Role.FOLLOWER;
                resetElectionTimeout(); // Nhận được heartbeat -> reset timeout
                resp.success = true;
            }
            return resp;
        }

        // Với SET/GET từ Client: Follower sẽ REDIRECT về phía Leader
        if ("SET".equals(msg.type) || "GET".equals(msg.type)) {
            if (role != Role.LEADER) {
                Message redirect = new Message();
                redirect.status = "REDIRECT";
                redirect.leaderAddress = votedFor != null ? peers.get(votedFor) : "";
                return redirect;
            }
            // Nếu là Leader, tạm thời phản hồi trực tiếp
            Message resp = new Message();
            if ("GET".equals(msg.type)) {
                resp.status = "VALUE";
                resp.value = stateMachine.get(msg.key);
            } else {
                stateMachine.put(msg.key, msg.value);
                resp.status = "OK";
            }
            return resp;
        }
        return null;
    }
```

### 🔍 Cách chạy thử nghiệm Feature 2:
1.  Khởi chạy 3 nút ở 3 terminal riêng biệt (Sử dụng script `./start_node.sh` đã viết):
    *   Terminal 1: `./start_node.sh 8001`
    *   Terminal 2: `./start_node.sh 8002`
    *   Terminal 3: `./start_node.sh 8003`
2.  Quan sát logs: Bạn sẽ thấy một nút báo: `Transitioned to LEADER for gen 1`. Hai nút còn lại báo nhận heartbeat.
3.  Gửi lệnh SET lên nút Follower (ví dụ 8002):
    `./client.sh 8002 SET name Gemini`
    *Kết quả trả về*: Phải là `REDIRECT` kèm địa chỉ của Leader.

---

## Feature 3: Ghi đĩa bền vững (Write-Ahead Log - WAL)
**Mục tiêu**: Ghi nhận thay đổi xuống file `node_x_wal.log` trước khi thực thi. Khi nút sập và khởi động lại, nó đọc file này để khôi phục State Machine cục bộ.

### Bước 3.1: Viết lớp xử lý file log `WalLogger.java`
Tạo file [src/main/java/org/phunn/WalLogger.java](file:///Users/nguyen/Documents/java/distributed_system/KeyValueStore/distributed_system/src/main/java/org/phunn/WalLogger.java) như mô tả ở bước thiết kế chung (nó chứa hàm `append()`, `load()`, và `truncate()`).

### Bước 3.2: Tích hợp WalLogger vào `Node.java`
Cập nhật phương thức `start()` để đọc file log và đưa dữ liệu cũ vào bộ nhớ:
```java
    // Sửa phương thức start() trong Node.java:
    public synchronized void start() throws IOException {
        running = true;
        
        // Tải lại logs từ ổ đĩa khi khởi động
        WalLogger wal = new WalLogger(walPath);
        this.log.addAll(wal.load());
        for (LogEntry e : this.log) {
            applyToStateMachine(e);
        }
        
        serverSocket = new ServerSocket(port);
        threadPool.execute(this::listen);
        threadPool.execute(this::runTimerLoop);
    }
```

Viết hàm `appendEntry()` để vừa ghi vào danh sách trong bộ nhớ, vừa ghi xuống đĩa:
```java
    private void appendEntry(LogEntry entry) {
        log.add(entry);
        try {
            new WalLogger(walPath).append(entry);
        } catch (IOException ignored) {}
    }
```

Cập nhật xử lý ghi `SET` của Client trên Leader để lưu log:
```java
            // Sửa khối xử lý "SET" của Client trong processMessage():
            } else {
                // Tạo một LogEntry mới có offset = log.size() + 1
                LogEntry entry = new LogEntry(log.size() + 1, currentGeneration, "SET " + msg.key + " " + msg.value);
                appendEntry(entry); // Ghi file log
                applyToStateMachine(entry); // Cập nhật map bộ nhớ cục bộ
                
                Message resp = new Message();
                resp.status = "OK";
                return resp;
            }
```

### 🔍 Cách chạy thử nghiệm Feature 3:
1.  Chạy cụm, tìm Leader và gửi một lệnh ghi:
    `./client.sh 8001 SET database redis`
2.  Kiểm tra xem file log `node_1_wal.log` có xuất hiện dòng:
    `1 1 SET database redis` hay không.
3.  Tắt nút bằng `Ctrl+C`. Sau đó bật lại nút đó.
4.  Kiểm tra xem giá trị `database` có tự động được nạp lại vào bộ nhớ mà không cần client gửi lệnh mới không:
    `./client.sh 8001 GET_LOCAL database` $\rightarrow$ Trả về `redis`.

---

## Feature 4: Nhân bản Log & Vạch nước cao (High-Water Mark)
**Mục tiêu**: Leader gửi các entry mới cho Followers. Khi nhận được $\ge 2$ xác nhận (Leader + 1 Follower đã ghi xong), Leader nâng `commitOffset` (Vạch nước cao), cập nhật map bộ nhớ của nó và trả lời Client. Đồng thời, Followers nhận được `leaderCommitOffset` cũng nâng vạch nước cao cục bộ và cập nhật Map bộ nhớ của tụi nó.

### Bước 4.1: Đồng bộ hóa thread Client và thread Heartbeat (Tránh deadlock)
Chúng ta cập nhật hàm xử lý của Leader để chặn Client lại cho đến khi dữ liệu được replicate qua quá bán. Để tránh chặn toàn bộ tiến trình, ta sử dụng `this.wait()` giải phóng lock tạm thời.

Sửa đổi phần xử lý `SET` trên Leader ở `processMessage()`:
```java
            } else {
                LogEntry entry = new LogEntry(log.size() + 1, currentGeneration, "SET " + msg.key + " " + msg.value);
                appendEntry(entry);
                
                // Đợi cho đến khi dòng log này được nâng qua vạch nước cao (commitOffset)
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
```

Thêm các phương thức đa luồng đồng bộ:
```java
    private synchronized boolean waitForCommit(long offset, long timeoutMs) {
        long start = System.currentTimeMillis();
        while (commitOffset < offset) {
            long elapsed = System.currentTimeMillis() - start;
            long remaining = timeoutMs - elapsed;
            if (remaining <= 0) break;
            try {
                this.wait(remaining); // Nhường lock cho thread heartbeat thực hiện gửi tin
            } catch (InterruptedException e) {
                break;
            }
        }
        return commitOffset >= offset;
    }

    private void checkAndUpdateCommitOffset() {
        long lastLogOffset = log.isEmpty() ? 0 : log.get(log.size() - 1).offset;
        for (long N = lastLogOffset; N > commitOffset; N--) {
            int count = 1; // Bản thân Leader
            for (long match : matchOffset.values()) {
                if (match >= N) count++;
            }
            if (count >= (peers.size() + 1) / 2 + 1) { // Đạt quá bán
                if (log.get((int)N - 1).generation == currentGeneration) {
                    commitOffset = N;
                    applyCommitted();
                    this.notifyAll(); // Đánh thức Client đang ngủ chờ phản hồi
                    break;
                }
            }
        }
    }

    private void applyCommitted() {
        for (long i = lastApplied + 1; i <= commitOffset; i++) {
            if (i <= log.size()) {
                applyToStateMachine(log.get((int)i - 1));
            }
        }
    }
```

### Bước 4.2: Gửi kèm Log Entries trong Heartbeats và xử lý ở Follower
Cập nhật phương thức `sendHeartbeats()` trên Leader để gửi kèm dữ liệu nhật ký mới:
```java
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
                                // Lệch log, giảm chỉ số gửi để quét tìm điểm khớp
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
```

Cập nhật `APPEND_ENTRIES` trên Follower ở `processMessage()` để nhận log và nâng vạch nước cao:
```java
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
                if (role != Role.FOLLOWER) role = Role.FOLLOWER;
                resetElectionTimeout();
                votedFor = msg.leaderId;

                // Kiểm tra log liền trước có khớp không (Consistency check)
                boolean hasPrev = msg.prevLogOffset == 0 || 
                                  (msg.prevLogOffset <= log.size() && 
                                   log.get((int)msg.prevLogOffset - 1).generation == msg.prevLogGeneration);

                if (hasPrev) {
                    resp.success = true;
                    long index = msg.prevLogOffset;
                    for (LogEntry newEntry : msg.entries) {
                        index++;
                        if (log.size() < index) {
                            appendEntry(newEntry); // Ghi log mới
                        }
                    }
                    resp.matchOffset = log.isEmpty() ? 0 : log.get(log.size() - 1).offset;

                    // Nhận được vạch nước cao mới từ Leader -> Nâng commitOffset cục bộ
                    if (msg.leaderCommitOffset > commitOffset) {
                        commitOffset = Math.min(msg.leaderCommitOffset, resp.matchOffset);
                        applyCommitted();
                        this.notifyAll();
                    }
                }
            }
            return resp;
        }
```

*Lưu ý loại bỏ việc tự ý gọi applyToStateMachine trực tiếp khi ghi SET. Map bộ nhớ lúc này chỉ được cập nhật thông qua hàm `applyCommitted()` khi và chỉ khi log được nâng qua vạch nước cao.*

### 🔍 Cách chạy thử nghiệm Feature 4:
1.  Bật 3 nút lên.
2.  Gửi yêu cầu ghi lên Leader:
    `./client.sh 8001 SET name Antigravity`
    *Kết quả trả về*: `{"status":"OK"}` (Ghi thành công vì có ít nhất 1 Follower đã ghi xong và gửi xác nhận về Leader).
3.  Kiểm tra xem giá trị `name` có tồn tại trong bộ nhớ Follower (ví dụ 8003) hay không:
    `./client.sh 8003 GET_LOCAL name` $\rightarrow$ Trả về `Antigravity`.

---

## Feature 5: Cắt log lệch (Log Truncation)
**Mục tiêu**: Khi một nút Leader cũ bị sập, các nút còn lại ghi thêm dữ liệu mới. Khi nút cũ khởi động lại, nó có log chứa các dòng bị sai Generation so với Leader mới. Nó phải phát hiện điểm sai lệch này, cắt bỏ toàn bộ log từ điểm sai lệch đó đi và ghi đè log mới của Leader mới.

### Bước 5.1: Thực hiện điều kiện Truncate khi ghi đè log ở Follower
Cập nhật vòng lặp `APPEND_ENTRIES` trên Follower để phát hiện sai lệch Generation và kích hoạt hàm `wal.truncate()`:

```java
                    // Cập nhật vòng lặp xử lý newEntry trong APPEND_ENTRIES của processMessage():
                    long index = msg.prevLogOffset;
                    for (LogEntry newEntry : msg.entries) {
                        index++;
                        if (log.size() >= index) {
                            LogEntry existing = log.get((int)index - 1);
                            if (existing.generation != newEntry.generation) {
                                // Phát hiện dòng log bị lệch kỳ (Generation)!
                                try {
                                    // Cắt file WAL trên đĩa từ vị trí index
                                    new WalLogger(walPath).truncate(index);
                                } catch (IOException ignored) {}
                                
                                // Xóa khỏi danh sách log trong bộ nhớ
                                while (log.size() >= index) {
                                    log.remove(log.size() - 1);
                                }
                                appendEntry(newEntry); // Ghi đè log đúng của Leader mới
                            }
                        } else {
                            appendEntry(newEntry);
                        }
                    }
```

### 🔍 Cách chạy thử nghiệm Feature 5:
1.  **Bước 1**: Bật 3 nút. Gửi lệnh:
    `./client.sh 8001 SET key1 val1`
    *(Nhật ký của 3 nút lúc này giống nhau ở offset 1)*.
2.  **Bước 2**: Tắt đột ngột nút Leader (ví dụ 8001) bằng `Ctrl+C`.
3.  **Bước 3**: Gửi lệnh SET mới lên Leader mới (ví dụ 8002):
    `./client.sh 8002 SET key1 val_divergent`
    *(Dữ liệu tại offset 2 được đồng bộ trên 8002 và 8003 ở thế hệ mới)*.
4.  **Bước 4**: Bật lại nút 8001: `./start_node.sh 8001`
    *(Nút 8001 nhận tin đồng bộ từ Leader 8002. Nó phát hiện dòng log tại offset 2 bị lệch kỳ, xóa bỏ log cũ và ghi đè dữ liệu mới)*.
5.  **Bước 5**: Kiểm tra xem nút 8001 đã tự sửa dữ liệu thành công chưa:
    `./client.sh 8001 GET_LOCAL key1` $\rightarrow$ Trả về `val_divergent`! (Nhật ký đã được cắt bỏ và đồng bộ thành công!).
