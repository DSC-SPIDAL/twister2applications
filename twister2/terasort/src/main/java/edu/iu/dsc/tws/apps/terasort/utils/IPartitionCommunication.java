package edu.iu.dsc.tws.apps.terasort.utils;


public interface IPartitionCommunication {

    void onSimpleSelection(int noOfTasks);

    void onRandomSelection();

    void onRandomSelection(int noOfTasks);

    void onRingSelection();

    void onCustomSelection();

}
