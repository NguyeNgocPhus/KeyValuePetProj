package org.phunn;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

class SetupTest {

    @Test
    void testJacksonAndPreviewFeatures() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String json = "{\"message\":\"hello\",\"value\":123}";
        
        // Deserialize using Jackson
        Map<?, ?> map = mapper.readValue(json, Map.class);
        assertEquals("hello", map.get("message"));
        assertEquals(123, map.get("value"));

        // Use a preview feature to verify compiler configuration.
        // For example, using pattern matching in switch or record patterns
        Object obj = "Java 25 Preview";
        String result = switch (obj) {
            case String s -> "String of length " + s.length();
            default -> "Unknown";
        };
        assertEquals("String of length 15", result);
    }
}
