package edu.iu.dsc.tws.apps.terasort.utils;


import java.util.ArrayList;

public class PartitionCommunication implements IPartitionCommunication {

    private int no_of_tasks;
    private ArrayList<Integer> selectedNodes = null;

    public PartitionCommunication(){

    }

    public PartitionCommunication(int noOfTasks){
        this.no_of_tasks = noOfTasks;
        selectedNodes = new ArrayList<>(noOfTasks);
    }

    @Override
    public int onSimpleSelection(int noOfTasks) {

        int startId = 1;



        return startId;

    }

    @Override
    public int onSimpleSelection(int startTask, int noOfTasks) {

        if(startTask >0 && noOfTasks >0){
            int randomStart = startTask % noOfTasks;

            return randomStart;
        }else{
            return 1;
        }

    }

    @Override
    public void onRandomSelection() {

    }

    @Override
    public void onRandomSelection(int noOfTasks) {

    }

    @Override
    public int onRandomSelection(int startTask, int noOfTasks) {



        return 0;
    }

    @Override
    public void onRingSelection() {

    }

    @Override
    public void onCustomSelection() {

    }
}
