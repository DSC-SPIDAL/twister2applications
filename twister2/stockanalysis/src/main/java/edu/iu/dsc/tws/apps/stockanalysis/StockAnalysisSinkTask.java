package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.modifiers.Collector;
import edu.iu.dsc.tws.api.task.nodes.BaseSink;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;

import java.util.logging.Logger;

public class StockAnalysisSinkTask extends BaseSink implements Collector {

    private static final Logger LOG = Logger.getLogger(StockAnalysisSinkTask.class.getName());
    @Override
    public boolean execute(IMessage content) {
        LOG.fine("message content:" + content.getContent());
        return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
        super.prepare(cfg, context);
    }

    @Override
    public DataPartition<double[][]> get() {
        return new EntityPartition<>(context.taskIndex(), null);
    }
}
