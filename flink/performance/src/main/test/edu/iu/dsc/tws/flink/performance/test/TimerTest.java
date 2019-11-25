package edu.iu.dsc.tws.flink.performance.test;

import edu.iu.dsc.tws.flinkapps.payload.SimpleTimerTask;
import org.junit.jupiter.api.Test;

import java.util.Timer;
import java.util.TimerTask;

public class TimerTest {

    @Test
    public void test1() {
        System.out.println("Simple Test");
        Timer timer = new Timer();
        TimerTask timerTask = new SimpleTimerTask();

        timer.schedule(timerTask, 200);
    }
}
