package edu.iu.dsc.tws.apps.slam.streaming.stats;

public class GCCounter {
    private long fullGCTime = 0;

    private long youngGCTime = 0;

    private String id;

    public GCCounter() {
    }

    public GCCounter(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getFullGCTime() {
        return fullGCTime;
    }

    public void setFullGCTime(long fullGCTime) {
        this.fullGCTime = fullGCTime;
    }

    public void addFullGCTime(long gcTime) {
        this.fullGCTime += gcTime;
    }

    public long getYoungGCTime() {
        return youngGCTime;
    }

    public void addYoungGCTime(long youngGCTime) {
        this.youngGCTime += youngGCTime;
    }
}
