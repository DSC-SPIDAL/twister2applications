package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.modifiers.Collector;
import edu.iu.dsc.tws.api.task.nodes.BaseSink;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;


public class DataPreprocessingSinkTask extends BaseSink implements Collector {

    private static final Logger LOG = Logger.getLogger(DataPreprocessingSinkTask.class.getName());

    private String vectorDirectory;
    private String distanceDirectory;
    private int distanceType;

    private Map<Integer, VectorPoint> currentPoints;
    private List<String> vectorsList = new ArrayList<>();
    private List<Map<Integer, VectorPoint>> values = new ArrayList<>();

    public DataPreprocessingSinkTask(String vectordirectory, String distancedirectory, int distancetype) {
        this.vectorDirectory = vectordirectory;
        this.distanceDirectory = distancedirectory;
        this.distanceType = distancetype;
    }

    @Override
    public boolean execute(IMessage content) {
        values.add((Map<Integer, VectorPoint>) content.getContent());
        LOG.info("Values Size:" + values.size());

        DistanceCalculator distanceCalculator = new DistanceCalculator(values, vectorDirectory,
                distanceDirectory, distanceType);
        distanceCalculator.process();
        //distanceCalculator.process(true);
        return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
        super.prepare(cfg, context);
    }

    @Override
    public DataPartition<List<Map<Integer, VectorPoint>>> get() {
        return new EntityPartition<>(context.taskIndex(), null);
    }
}