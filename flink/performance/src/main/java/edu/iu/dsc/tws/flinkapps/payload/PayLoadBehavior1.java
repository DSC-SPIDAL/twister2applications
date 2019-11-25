package edu.iu.dsc.tws.flinkapps.payload;

import java.util.Timer;
import java.util.TimerTask;

@Deprecated
public class PayLoadBehavior1 extends DynamicLoadGenerator {

    public static PayLoadBehavior1 behavior1;

    private Timer timer;

    private PayLoadTask timerTask;

    static {
        behavior1 = new PayLoadBehavior1(100, 256, 500);
    }


    public PayLoadBehavior1(long loadChangePeriod, int loadSize, long totalLoadGenerationTime) {
        super(loadChangePeriod, loadSize, totalLoadGenerationTime);
        this.timer = new Timer();
        this.timerTask = new PayLoadTask1(loadSize, 100);
    }

    @Override
    public void generateLoad() {

        synchronized (behavior1) {
            try {
                behavior1.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.timer.cancel();

            System.out.println("Purge All Removed Tasks : " + this.timer.purge());

        }
    }

    public PayLoadTask getTimerTask() {
        return timerTask;
    }
}
