package edu.iu.dsc.tws.apps.slam.streaming.msgs;

public class Ready {
    private int taskId;

    private Trace trace;

    public Ready() {
    }

    public Trace getTrace() {
        return trace;
    }

    public void setTrace(Trace trace) {
        this.trace = trace;
    }

    public Ready(int taskId) {
        this.taskId = taskId;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }
}
