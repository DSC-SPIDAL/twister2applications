package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;

import java.util.List;
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
        LOG.info("%%%%%%%%%%%% Received message:" + content.getContent());
        if (content.getContent() != null) {
            List<Short> distanceMatrix = (List<Short>) content.getContent();
            LOG.info("Received message:" + distanceMatrix.size());
        }
        run(); //run() method to invoke the mds processing
        context.write(edgeName, "received distance for processing mds");
        return true;
    }
}
