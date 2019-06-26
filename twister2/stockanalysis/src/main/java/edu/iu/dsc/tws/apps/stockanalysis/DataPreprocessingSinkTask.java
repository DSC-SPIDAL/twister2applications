package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.modifiers.Collector;
import edu.iu.dsc.tws.api.task.nodes.BaseSink;

import java.util.logging.Logger;

public class DataPreprocessingSinkTask extends BaseSink implements Collector {

    private static final Logger LOG = Logger.getLogger(DataPreprocessingSinkTask.class.getName());

    private String vectorDirectory;
    private String distanceDirectory;
    private String distanceType;

    public DataPreprocessingSinkTask(String vectordirectory, String distancedirectory, String distancetype) {
        this.vectorDirectory = vectordirectory;
        this.distanceDirectory = distancedirectory;
        this.distanceType = distancetype;
    }

    @Override
    public DataPartition<?> get() {
        return null;
    }

    @Override
    public boolean execute(IMessage content) {
        LOG.info("Received message:" + content.getContent().toString());
        DistanceCalculator distanceCalculator = new DistanceCalculator(vectorDirectory, distanceDirectory,
                Integer.parseInt(distanceType));
        distanceCalculator.process();
        return true;
    }
}