package edu.iu.dsc.tws.flinkapps.payload.test;

import edu.iu.dsc.tws.flinkapps.payload.SimpleTimerTask;

import java.util.Timer;
import java.util.TimerTask;

public class TestVariablePayLoad {

    public static TestVariablePayLoad testVariablePayLoad;

    public static void main(String[] args) throws InterruptedException {
        testVariablePayLoad = new TestVariablePayLoad();
        System.out.println("Simple Test");
        Timer timer = new Timer();
        TimerTask timerTask = new SimpleTimerTask();
        timer.schedule(timerTask, 100, 100);
        System.out.println("Timer Started...");

        synchronized (testVariablePayLoad) {

            testVariablePayLoad.wait();

            timer.cancel();

            System.out.println("Purge All Removed Tasks : " + timer.purge());
        }


    }
}
