package edu.iu.dsc.tws.apps.terasort.utils;


import java.util.List;

public interface IPartitionCommunication {

    int onSimpleSelection(int noOfTasks);

    int onSimpleSelection(int startTask, int noOfTasks);

    List<Integer> onRandomSelection();

    void onRandomSelection(int noOfTasks);

    int onRandomSelection(int startTask, int noOfTasks);

    void onRingSelection();

    void onCustomSelection();

}
