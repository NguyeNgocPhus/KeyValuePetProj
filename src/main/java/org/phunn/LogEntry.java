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
