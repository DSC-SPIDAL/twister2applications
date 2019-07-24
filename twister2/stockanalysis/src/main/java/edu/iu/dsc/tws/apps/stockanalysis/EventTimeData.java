package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.apps.stockanalysis.utils.Record;
import edu.iu.dsc.tws.task.window.api.ITimeStampedData;

import java.util.List;

public class EventTimeData implements ITimeStampedData<Record> {

    private long eventTime;
    private Record record;

    public List<Record> getRecordList() {
        return recordList;
    }

    private List<Record> recordList;
    private int id;

    public EventTimeData(List<Record> data, int id, long eventTime) {
        this.recordList = data;
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
}
