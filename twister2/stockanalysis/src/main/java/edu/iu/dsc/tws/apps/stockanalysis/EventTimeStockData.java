package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.apps.stockanalysis.utils.Record;
import edu.iu.dsc.tws.task.window.api.ITimeStampedData;

public class EventTimeStockData implements ITimeStampedData<Record> {

    private long eventTime;
    private Record record;
    private int id;

    public EventTimeStockData(Record data, int id, long eventTime) {
        this.record = data;
        this.id = id;
        this.eventTime = eventTime;
    }

    @Override
    public long getTime() {
        return this.eventTime;
    }

    @Override
    public Record getData() {
        return this.record;
    }

    public int getId() {
        return this.id;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public void setRecord(Record record) {
        this.record = record;
    }

    public void setId(int id) {
        this.id = id;
    }
}
