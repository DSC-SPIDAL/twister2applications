package edu.iu.dsc.tws.apps.terasort.utils;


public class PartitionCommunication implements IPartitionCommunication {

    @Override
    public int onSimpleSelection(int noOfTasks) {

        int startId = 1;



        return startId;

    }

    @Override
    public int onSimpleSelection(int startTask, int noOfTasks) {

        if(startTask >0 && noOfTasks >0){
            int randomStart = startTask % noOfTasks;;

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
    public void onRingSelection() {

    }

    @Override
    public void onCustomSelection() {

    }
}
