package edu.iu.dsc.tws.apps.terasort.utils;


import java.util.ArrayList;
import java.util.Random;

public class PartitionCommunication implements IPartitionCommunication {

    private int no_of_tasks;
    private int start_id;
    private ArrayList<Integer> selectedNodes = null;

    public PartitionCommunication(){

    }

    public PartitionCommunication(int start, int noOfTasks){
        this.no_of_tasks = noOfTasks;
        this.start_id = start;
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
    public ArrayList<Integer> onRandomSelection() {
        Random random = new Random();
        while (selectedNodes.size()==this.no_of_tasks) {
            int node = random.nextInt(this.no_of_tasks);
            if(!selectedNodes.contains(node)){
                selectedNodes.add(node);
            }
        }
        return selectedNodes;
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
