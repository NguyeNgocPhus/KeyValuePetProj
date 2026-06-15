package org.phunn;

import java.util.List;

public class Message {
    public String type; // APPEND_ENTRIES, APPEND_ENTRIES_RESPONSE, REQUEST_VOTE, REQUEST_VOTE_RESPONSE, SET, GET
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
    public String status; // OK, VALUE, REDIRECT, ERROR
    public String leaderAddress;
    public String message;

    public Message() {}
}
