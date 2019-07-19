package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Record;
import edu.iu.dsc.tws.task.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.window.collectives.ProcessWindow;
import edu.iu.dsc.tws.task.window.function.ProcessWindowedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class DataProcessingStreamingWindowCompute extends ProcessWindow {

    private static final long serialVersionUID = -343442338342343424L;

    private static final Logger LOG = Logger.getLogger(DataProcessingStreamingWindowCompute.class.getName());

    private OperationMode operationMode;
    private List<Record> messageList;

    public DataProcessingStreamingWindowCompute(ProcessWindowedFunction processWindowedFunction) {
        super(processWindowedFunction);
    }

    public DataProcessingStreamingWindowCompute(ProcessWindowedFunction processWindowedFunction,
                                                OperationMode operationMode) {
        super(processWindowedFunction);
        this.operationMode = operationMode;
    }

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
        super.prepare(cfg, ctx);
    }

    @Override
    public boolean process(IWindowMessage windowMessage) {
        messageList = new ArrayList();
        LOG.info("Received Message:" + windowMessage.getWindow());
        context.write(Context.TWISTER2_DIRECT_EDGE, messageList);
        return true;
    }

    @Override
    public boolean processLateMessages(IMessage lateMessage) {
        return false;
    }
}
