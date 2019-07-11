package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.nodes.BaseCompute;

import java.util.logging.Logger;

public class MDSProgramWorkerCompute extends BaseCompute {

    private static final Logger LOG = Logger.getLogger(MDSProgramWorkerCompute.class.getName());

    private String edgeName;

    public MDSProgramWorkerCompute(String edgename) {
       this.edgeName = edgename;
    }

    public void run() {
        LOG.info("I am inside run method");
    }

    @Override
    public boolean execute(IMessage content) {
        LOG.info("Received message:" + content.getContent());
        run(); //run() method to do the mds processing
        context.write(edgeName, "received distance for processing mds");
        return true;
    }
}
