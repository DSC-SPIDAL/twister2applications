package edu.iu.dsc.tws.apps.mds;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;

import java.util.logging.Logger;

public class DataGeneratorSink extends BaseCompute {

    private static final Logger LOG = Logger.getLogger(DataGeneratorSink.class.getName());

    public DataGeneratorSink() {
    }

    @Override
    public boolean execute(IMessage content) {
        LOG.info("Received Data Generation Input Message");
        return false;
    }
}
