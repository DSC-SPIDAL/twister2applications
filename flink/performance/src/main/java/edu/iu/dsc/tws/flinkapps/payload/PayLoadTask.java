package edu.iu.dsc.tws.flinkapps.payload;

import edu.iu.dsc.tws.flinkapps.data.CollectiveData;

import java.util.TimerTask;

@Deprecated
public abstract class PayLoadTask extends TimerTask {

    public int payLoadSize;

    public int maxPayLoadSize;

    private int counter = 0;

    public PayLoadTask(int payLoadSize) {
        this.payLoadSize = payLoadSize;
    }

    public PayLoadTask(int payLoadSize, int maxPayLoadSize) {
        this.payLoadSize = payLoadSize;
        this.maxPayLoadSize = maxPayLoadSize;
    }

    @Override
    public void run() {
        if (counter < maxPayLoadSize) {
            getLoad();
        } else {
            synchronized (PayLoadBehavior1.behavior1) {
                PayLoadBehavior1.behavior1.notify();
            }
        }
        counter++;
    }

    public abstract CollectiveData getLoad();
}
