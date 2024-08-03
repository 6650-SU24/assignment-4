package org.example;

public class Record {
    private long start;
    private String reqType;
    private long latency;
    private Integer respCode;

    public Record(long start, String reqType, long latency, Integer respCode) {
        this.start = start;
        this.reqType = reqType;
        this.latency = latency;
        this.respCode = respCode;
    }

    public long getStart() {
        return start;
    }

    public String getReqType() {
        return reqType;
    }

    public long getLatency() {
        return latency;
    }

    public Integer getRespCode() {
        return respCode;
    }

    public String toCsv() {
        return start + "," + reqType + "," + latency + "," + respCode;
    }
}
