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
        assertEquals(1, entries.get(0).generation);
        assertEquals(2, entries.get(1).offset);
    }

    @Test
    public void testTruncate() throws IOException {
        logger.append(new LogEntry(1, 1, "SET x 10"));
        logger.append(new LogEntry(2, 1, "SET y 20"));
        logger.append(new LogEntry(3, 2, "SET z 30"));

        // Truncate to offset 2 (meaning delete offset >= 2)
        logger.truncate(2);

        List<LogEntry> entries = logger.load();
        assertEquals(1, entries.size());
        assertEquals("SET x 10", entries.get(0).cmd);
    }
}
