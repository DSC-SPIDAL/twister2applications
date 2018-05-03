package edu.iu.dsc.tws.apps.slam.streaming.msgs;

public class ParticleAssignment {
    private int previousIndex;
    private int newIndex;

    private int previousTask;

    private int newTask;

    public ParticleAssignment() {
    }

    public ParticleAssignment(int previousIndex, int newIndex, int previousTask, int newTask) {
        this.previousIndex = previousIndex;
        this.newIndex = newIndex;
        this.previousTask = previousTask;
        this.newTask = newTask;
    }

    public int getPreviousIndex() {
        return previousIndex;
    }

    public int getNewIndex() {
        return newIndex;
    }

    public int getPreviousTask() {
        return previousTask;
    }

    public int getNewTask() {
        return newTask;
    }

    public void setPreviousIndex(int previousIndex) {
        this.previousIndex = previousIndex;
    }

    public void setNewIndex(int newIndex) {
        this.newIndex = newIndex;
    }

    public void setPreviousTask(int previousTask) {
        this.previousTask = previousTask;
    }

    public void setNewTask(int newTask) {
        this.newTask = newTask;
    }

    @Override
    public String toString() {
        return "ParticleAssignment{" +
                "previousIndex=" + previousIndex +
                ", newIndex=" + newIndex +
                ", previousTask=" + previousTask +
                ", newTask=" + newTask +
                '}';
    }
}
