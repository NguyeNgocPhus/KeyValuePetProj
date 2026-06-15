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
