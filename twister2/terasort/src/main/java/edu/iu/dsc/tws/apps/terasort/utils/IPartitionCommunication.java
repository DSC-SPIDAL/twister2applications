package edu.iu.dsc.tws.apps.terasort.utils;


public interface IPartitionCommunication {

    int onSimpleSelection(int noOfTasks);

    int onSimpleSelection(int startTask, int noOfTasks);

    void onRandomSelection();

    void onRandomSelection(int noOfTasks);

    int onRandomSelection(int startTask, int noOfTasks);

    void onRingSelection();

    void onCustomSelection();

}
