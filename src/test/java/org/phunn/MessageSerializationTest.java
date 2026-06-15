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
