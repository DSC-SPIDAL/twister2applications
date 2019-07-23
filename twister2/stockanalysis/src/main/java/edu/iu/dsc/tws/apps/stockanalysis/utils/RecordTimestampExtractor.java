package edu.iu.dsc.tws.apps.stockanalysis.utils;

import edu.iu.dsc.tws.task.window.api.TimestampExtractor;

import java.util.logging.Logger;

public class RecordTimestampExtractor extends TimestampExtractor<Record> {
    private static final Logger LOG = Logger.getLogger(RecordTimestampExtractor.class.getName());

    @Override
    public long extractTimestamp(Record o) {
        LOG.info("time unit:" + o.getDate());
        return o.getDate().getTime();
    }
}
