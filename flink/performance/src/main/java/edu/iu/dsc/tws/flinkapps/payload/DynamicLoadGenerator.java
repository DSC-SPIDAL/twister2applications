package edu.iu.dsc.tws.flinkapps.payload;

import edu.iu.dsc.tws.flinkapps.data.CollectiveData;

import java.util.TimerTask;

@Deprecated
public abstract class DynamicLoadGenerator {

    private long loadChangePeriod;
    private int loadSize;
    private long totalLoadGenerationTime;

    public DynamicLoadGenerator(long loadChangePeriod, int loadSize, long totalLoadGenerationTime) {
        this.loadChangePeriod = loadChangePeriod;
        this.loadSize = loadSize;
        this.totalLoadGenerationTime = totalLoadGenerationTime;
    }

    public abstract void generateLoad();

    public abstract PayLoadTask getTimerTask();

}
