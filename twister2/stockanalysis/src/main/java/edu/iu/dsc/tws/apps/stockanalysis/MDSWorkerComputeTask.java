package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.nodes.BaseCompute;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;

import java.util.logging.Logger;

public class MDSWorkerComputeTask extends BaseCompute {

    private static final Logger LOG = Logger.getLogger(MDSWorkerComputeTask.class.getName());

    private String edgeName;

    public MDSWorkerComputeTask(String edgename) {
        this.edgeName = edgename;
    }

    public void run() {
        LOG.info("I am inside run method");
    }

    @Override
    public boolean execute(IMessage content) {

        if (content.getContent() != null) {
            String vectorPoint = (String) content.getContent();
            LOG.info("Received message:" + vectorPoint);
        }
        //run(); //run() method to invoke the mds processing
        context.write(edgeName, "received distance for processing mds");
        return true;
    }
}
