package edu.iu.dsc.tws.flinkapps.payload;

import edu.iu.dsc.tws.flinkapps.payload.test.TestVariablePayLoad;

import java.util.TimerTask;

public class SimpleTimerTask extends TimerTask {

    private final int threshold = 10;

    public static int counter = 0;

    @Override
    public void run() {
        if (counter < threshold) {
            System.out.println("Timer Ran " + counter);
        } else {
            synchronized (TestVariablePayLoad.testVariablePayLoad) {
                TestVariablePayLoad.testVariablePayLoad.notify();
            }
        }
        counter++;

    }
}
