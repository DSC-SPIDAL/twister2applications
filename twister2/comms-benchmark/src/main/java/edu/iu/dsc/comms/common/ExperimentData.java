package edu.iu.dsc.comms.common;

import edu.iu.dsc.tws.task.graph.OperationMode;

import java.util.List;


public class ExperimentData {

    private Object input;
    private Object output;
    private List<Integer> taskStages;
    private int workerId;
    private int numOfWorkers;
    private int taskId;
    private int iterations;
    private OperationMode operationMode;

    public ExperimentData() {

    }

    public Object getInput() {
        return input;
    }

    public void setInput(Object input) {
        this.input = input;
    }

    public Object getOutput() {
        return output;
    }

    public void setOutput(Object output) {
        this.output = output;
    }

    public List<Integer> getTaskStages() {
        return taskStages;
    }

    public void setTaskStages(List<Integer> taskList) {
        this.taskStages = taskList;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void setWorkerId(int workerId) {
        this.workerId = workerId;
    }

    public OperationMode getOperationMode() {
        return operationMode;
    }

    public void setOperationMode(OperationMode operationMode) {
        this.operationMode = operationMode;
    }

    public int getIterations() {
        return iterations;
    }

    public void setIterations(int iterations) {
        this.iterations = iterations;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getNumOfWorkers() {
        return numOfWorkers;
    }

    public void setNumOfWorkers(int numOfWorkers) {
        this.numOfWorkers = numOfWorkers;
    }
}

