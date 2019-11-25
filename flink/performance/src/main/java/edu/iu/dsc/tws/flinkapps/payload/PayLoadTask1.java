package edu.iu.dsc.tws.flinkapps.payload;

import edu.iu.dsc.tws.flinkapps.data.CollectiveData;

@Deprecated
public class PayLoadTask1 extends PayLoadTask {

    private static int counter = 0;

    public PayLoadTask1(int payLoadSize, int maxPayLoadSize) {
        super(payLoadSize, maxPayLoadSize);
    }

    public PayLoadTask1(int payLoadSize, int maxPayLoadSize, int counter) {
        super(payLoadSize, maxPayLoadSize);
        this.counter = counter;
    }

    @Override
    public CollectiveData getLoad() {
        return new CollectiveData(payLoadSize, counter++);
    }
}
