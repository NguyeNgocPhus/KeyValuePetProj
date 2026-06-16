# Metadata Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist key metadata (`commitOffset`, `currentGeneration`, `votedFor`) to a properties file and reload it upon node startup to ensure correct state machine recovery from WAL.

**Architecture:** We will compute a metadata file path by appending `.meta` to the node's `walPath` (e.g., `node_1.log.meta`). We will use `java.util.Properties` to store and load metadata. Metadata will be persisted atomically in synchronized blocks whenever key variables change, and loaded in `start()` before processing the WAL.

**Tech Stack:** Java 25, JUnit 5

---

### Task 1: Add saveMetadata and loadMetadata to Node.java

**Files:**
- Modify: `src/main/java/org/phunn/Node.java`

- [ ] **Step 1: Write helper methods saveMetadata and loadMetadata**
  Add the helper methods to `Node.java` at the bottom of the file (before the closing brace of the `Node` class).

  ```java
      private void saveMetadata() {
          Properties props = new Properties();
          props.setProperty("commitOffset", String.valueOf(commitOffset));
          props.setProperty("currentGeneration", String.valueOf(currentGeneration));
          props.setProperty("votedFor", votedFor == null ? "" : String.valueOf(votedFor));
          
          String metaPath = walPath + ".meta";
          try (FileOutputStream out = new FileOutputStream(metaPath)) {
              props.store(out, "Node Metadata");
          } catch (IOException e) {
              System.err.println("[Node " + id + "] Failed to save metadata: " + e.getMessage());
          }
      }

      private void loadMetadata() {
          String metaPath = walPath + ".meta";
          File file = new File(metaPath);
          if (!file.exists()) {
              return;
          }
          Properties props = new Properties();
          try (FileInputStream in = new FileInputStream(file)) {
              props.load(in);
              this.commitOffset = Long.parseLong(props.getProperty("commitOffset", "0"));
              this.currentGeneration = Long.parseLong(props.getProperty("currentGeneration", "0"));
              String votedForStr = props.getProperty("votedFor", "");
              this.votedFor = votedForStr.isEmpty() ? null : Integer.parseInt(votedForStr);
              System.out.println("[Node " + id + "] Loaded metadata: commitOffset=" + commitOffset + ", currentGeneration=" + currentGeneration + ", votedFor=" + votedFor);
          } catch (IOException | NumberFormatException e) {
              System.err.println("[Node " + id + "] Failed to load metadata: " + e.getMessage());
          }
      }
  ```

- [ ] **Step 2: Load metadata upon start**
  Modify `start()` method in `Node.java` to call `loadMetadata()` before reading the WAL.

  ```java
      public synchronized void start() throws IOException {
          running = true;
          loadMetadata();
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
  ```

- [ ] **Step 3: Save metadata on state changes**
  Call `saveMetadata()` inside `stepDown()`, `startElection()`, and when changing `commitOffset` or `votedFor`.

  Modify `stepDown(long higherGeneration)`:
  ```java
      private synchronized void stepDown(long higherGeneration) {
          role = Role.FOLLOWER;
          currentGeneration = higherGeneration;
          votedFor = null;
          System.out.println("[Node " + id + "] Stepped down to FOLLOWER for generation " + currentGeneration);
          resetElectionTimeout();
          saveMetadata();
      }
  ```

  Modify `startElection()`:
  ```java
      private void startElection() {
          role = Role.CANDIDATE;
          currentGeneration++;
          votedFor = id;
          resetElectionTimeout();
          saveMetadata();
          ...
  ```

  Modify `processMessage()` for `REQUEST_VOTE` when vote is granted:
  ```java
              if (msg.generation == currentGeneration && logOk && (votedFor == null || votedFor == msg.candidateId)) {
                  resp.voteGranted = true;
                  votedFor = msg.candidateId;
                  System.out.println("[Node " + id + "] Voted for Candidate " + msg.candidateId + " for gen " + currentGeneration);
                  resetElectionTimeout();
                  saveMetadata();
              }
  ```

  Modify `processMessage()` for `APPEND_ENTRIES` when leader commit index advances:
  ```java
                     // Commit advancement
                      if (msg.leaderCommitOffset > commitOffset) {
                          commitOffset = Math.min(msg.leaderCommitOffset, resp.matchOffset);
                          applyCommitted();
                          saveMetadata();
                          this.notifyAll();
                      }
  ```

  Modify `checkAndUpdateCommitOffset()` when leader advances its commit offset:
  ```java
              if (count >= (peers.size() + 1) / 2 + 1) {
                  if (log.get((int)N - 1).generation == currentGeneration) {
                      System.out.println("[Node " + id + "] Advancing commitOffset up to " + N + " (majority match, gen matches current)");
                      commitOffset = N;
                      applyCommitted();
                      saveMetadata();
                      this.notifyAll(); // Wake up client threads waiting in waitForCommit
                      break;
                  }
  ```

- [ ] **Step 4: Verify Compilation**
  Run: `mvn clean compile`
  Expected: BUILD SUCCESS

---

### Task 2: Create NodeMetadataTest to verify loading/saving and recovery

**Files:**
- Create: `src/test/java/org/phunn/NodeMetadataTest.java`

- [ ] **Step 1: Write a unit test for metadata save/load**
  Create the file `src/test/java/org/phunn/NodeMetadataTest.java` with tests for:
  1. Saving and loading metadata to/from disk.
  2. Starting a Node with preexisting WAL and metadata to verify the State Machine recovers committed entries but ignores uncommitted ones.

  ```java
  package org.phunn;

  import org.junit.jupiter.api.AfterEach;
  import org.junit.jupiter.api.BeforeEach;
  import org.junit.jupiter.api.Test;
  import java.io.File;
  import java.io.IOException;
  import java.util.Map;
  import static org.junit.jupiter.api.Assertions.*;

  public class NodeMetadataTest {
      private String walPath = "test_node_meta.log";
      private String metaPath = "test_node_meta.log.meta";

      @BeforeEach
      @AfterEach
      public void cleanUp() {
          new File(walPath).delete();
          new File(metaPath).delete();
      }

      @Test
      public void testMetadataSaveAndLoad() throws IOException {
          Node node = new Node(1, 19999, Map.of(), walPath);
          node.currentGeneration = 3;
          node.votedFor = 2;
          node.commitOffset = 4;
          
          node.start();
          
          // Trigger a save by stepping down
          node.stepDown(5); // sets generation to 5, votedFor to null, and saves meta
          node.stop();
          
          Node node2 = new Node(1, 19999, Map.of(), walPath);
          node2.start(); // calls loadMetadata()
          
          assertEquals(5, node2.currentGeneration);
          assertNull(node2.votedFor);
          assertEquals(4, node2.commitOffset);
          
          node2.stop();
      }

      @Test
      public void testStateMachineRecovery() throws IOException {
          // Pre-populate WAL with 3 entries
          WalLogger wal = new WalLogger(walPath);
          wal.append(new LogEntry(1, 1, "SET x 100"));
          wal.append(new LogEntry(2, 1, "SET y 200"));
          wal.append(new LogEntry(3, 1, "SET z 300"));

          // Suppose only up to offset 2 was committed
          java.util.Properties props = new java.util.Properties();
          props.setProperty("commitOffset", "2");
          props.setProperty("currentGeneration", "1");
          props.setProperty("votedFor", "");
          try (java.io.FileOutputStream out = new java.io.FileOutputStream(metaPath)) {
              props.store(out, "Node Metadata");
          }

          Node node = new Node(1, 19999, Map.of(), walPath);
          node.start();

          // State machine should only have x and y, but not z
          assertEquals("100", node.stateMachine.get("x"));
          assertEquals("200", node.stateMachine.get("y"));
          assertNull(node.stateMachine.get("z"));
          
          assertEquals(2, node.commitOffset);
          assertEquals(2, node.lastApplied);

          node.stop();
      }
  }
  ```

- [ ] **Step 2: Run tests to verify they pass**
  Run: `mvn test -Dtest=NodeMetadataTest`
  Expected: BUILD SUCCESS (all tests pass)

- [ ] **Step 3: Run full integration/unit tests suite**
  Run: `mvn clean test`
  Expected: BUILD SUCCESS (all existing cluster tests and metadata tests pass)
