package org.phunn;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

public class NodeMetadataTest {
    private File tempWalFile;
    private File tempMetaFile;
    private Node node;

    @BeforeEach
    public void setUp() throws IOException {
        tempWalFile = File.createTempFile("node_test", ".log");
        tempMetaFile = new File(tempWalFile.getAbsolutePath() + ".meta");
        node = new Node(1, 12345, new HashMap<>(), tempWalFile.getAbsolutePath());
    }

    @AfterEach
    public void tearDown() {
        if (tempWalFile.exists()) {
            tempWalFile.delete();
        }
        if (tempMetaFile.exists()) {
            tempMetaFile.delete();
        }
    }

    @Test
    public void testSaveMetadata() throws Exception {
        node.currentGeneration = 5;
        node.commitOffset = 10;
        node.votedFor = 2;

        Method saveMethod = Node.class.getDeclaredMethod("saveMetadata");
        saveMethod.setAccessible(true);
        saveMethod.invoke(node);

        assertTrue(tempMetaFile.exists(), "Metadata file should be created");

        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(tempMetaFile)) {
            props.load(in);
        }

        assertEquals("5", props.getProperty("currentGeneration"));
        assertEquals("10", props.getProperty("commitOffset"));
        assertEquals("2", props.getProperty("votedFor"));
    }

    @Test
    public void testLoadMetadata() throws Exception {
        Properties props = new Properties();
        props.setProperty("currentGeneration", "8");
        props.setProperty("commitOffset", "15");
        props.setProperty("votedFor", "3");

        try (java.io.FileOutputStream out = new java.io.FileOutputStream(tempMetaFile)) {
            props.store(out, "Test Metadata");
        }

        Method loadMethod = Node.class.getDeclaredMethod("loadMetadata");
        loadMethod.setAccessible(true);
        loadMethod.invoke(node);

        assertEquals(8, node.currentGeneration);
        assertEquals(15, node.commitOffset);
        assertEquals(3, node.votedFor);
    }

    @Test
    public void testLoadMetadataOnStart() throws Exception {
        // Create a WAL file with some log entries
        WalLogger wal = new WalLogger(tempWalFile.getAbsolutePath());
        wal.append(new LogEntry(1, 1, "SET x 10"));
        wal.append(new LogEntry(2, 1, "SET y 20"));

        // Create metadata file with commitOffset = 1
        Properties props = new Properties();
        props.setProperty("currentGeneration", "2");
        props.setProperty("commitOffset", "1");
        props.setProperty("votedFor", "1");

        try (java.io.FileOutputStream out = new java.io.FileOutputStream(tempMetaFile)) {
            props.store(out, "Test Metadata");
        }

        // Start node on port 0 (ephemeral port)
        Node startNode = new Node(1, 0, new HashMap<>(), tempWalFile.getAbsolutePath());
        try {
            startNode.start();
            
            // Verify metadata is loaded
            assertEquals(2, startNode.currentGeneration);
            assertEquals(1, startNode.commitOffset);
            assertEquals(1, startNode.votedFor);
            
            // Verify only entry 1 is applied (since commitOffset is 1)
            assertEquals("10", startNode.stateMachine.get("x"));
            assertNull(startNode.stateMachine.get("y"));
        } finally {
            startNode.stop();
        }
    }

    @Test
    public void testStepDownSavesMetadata() throws Exception {
        node.currentGeneration = 1;
        node.votedFor = 1;

        Method stepDownMethod = Node.class.getDeclaredMethod("stepDown", long.class);
        stepDownMethod.setAccessible(true);
        stepDownMethod.invoke(node, 3L);

        // Verify that the metadata was saved
        assertTrue(tempMetaFile.exists());
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(tempMetaFile)) {
            props.load(in);
        }
        assertEquals("3", props.getProperty("currentGeneration"));
        assertEquals("", props.getProperty("votedFor")); // votedFor should be null -> empty string
    }

    @Test
    public void testStartElectionSavesMetadata() throws Exception {
        node.currentGeneration = 2;
        node.votedFor = null;

        Method startElectionMethod = Node.class.getDeclaredMethod("startElection");
        startElectionMethod.setAccessible(true);
        startElectionMethod.invoke(node);

        // Verify that the metadata was saved
        assertTrue(tempMetaFile.exists());
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(tempMetaFile)) {
            props.load(in);
        }
        assertEquals("3", props.getProperty("currentGeneration"));
        assertEquals("1", props.getProperty("votedFor")); // votedFor should be self node.id (1)
    }

    @Test
    public void testRequestVoteSavesMetadata() throws Exception {
        node.currentGeneration = 4;
        node.votedFor = null;

        Message msg = new Message();
        msg.type = "REQUEST_VOTE";
        msg.generation = 4;
        msg.candidateId = 2;
        msg.lastLogOffset = 0;
        msg.lastLogGeneration = 0;

        Message resp = node.processMessage(msg);
        assertTrue(resp.voteGranted);
        assertEquals(Integer.valueOf(2), node.votedFor);

        // Verify that the metadata was saved
        assertTrue(tempMetaFile.exists());
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(tempMetaFile)) {
            props.load(in);
        }
        assertEquals("4", props.getProperty("currentGeneration"));
        assertEquals("2", props.getProperty("votedFor"));
    }

    @Test
    public void testAppendEntriesCommitAdvancementSavesMetadata() throws Exception {
        node.currentGeneration = 4;
        node.commitOffset = 0;
        
        // Add a log entry so matching offset can advance
        node.log.add(new LogEntry(1, 4, "SET x 10"));

        Message msg = new Message();
        msg.type = "APPEND_ENTRIES";
        msg.generation = 4;
        msg.leaderId = 3;
        msg.prevLogOffset = 0;
        msg.prevLogGeneration = 0;
        msg.entries = new ArrayList<>();
        msg.leaderCommitOffset = 1;

        Message resp = node.processMessage(msg);
        assertTrue(resp.success);
        assertEquals(1, node.commitOffset);

        // Verify that the metadata was saved
        assertTrue(tempMetaFile.exists());
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(tempMetaFile)) {
            props.load(in);
        }
        assertEquals("1", props.getProperty("commitOffset"));
    }

    @Test
    public void testCheckAndUpdateCommitOffsetSavesMetadata() throws Exception {
        node.role = Node.Role.LEADER;
        node.currentGeneration = 4;
        node.commitOffset = 0;
        
        // Add log entry
        node.log.add(new LogEntry(1, 4, "SET x 10"));
        // Match offset for peer
        node.peers.put(2, "localhost:12346");
        node.matchOffset.put(2, 1L);

        Method checkMethod = Node.class.getDeclaredMethod("checkAndUpdateCommitOffset");
        checkMethod.setAccessible(true);
        synchronized (node) {
            checkMethod.invoke(node);
        }

        assertEquals(1, node.commitOffset);

        // Verify that the metadata was saved
        assertTrue(tempMetaFile.exists());
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(tempMetaFile)) {
            props.load(in);
        }
        assertEquals("1", props.getProperty("commitOffset"));
    }
}
