package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.modifiers.Collector;
import edu.iu.dsc.tws.api.task.nodes.BaseSink;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;

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
    //private List<VectorPoint> vectorsList = new ArrayList<>();
    private List<String> vectorsList = new ArrayList<>();
    List<Map<Integer, VectorPoint>> values = new ArrayList<>();

    public DataPreprocessingSinkTask(String vectordirectory, String distancedirectory, int distancetype) {
        this.vectorDirectory = vectordirectory;
        this.distanceDirectory = distancedirectory;
        this.distanceType = distancetype;
    }

    @Override
    public boolean execute(IMessage content) {
        //currentPoints = (Map<Integer, VectorPoint>) content.getContent();
        //LOG.info("%%%% Current Points value: %%%%" + content.getContent());

        values.add((Map<Integer, VectorPoint>) content.getContent());
        LOG.info("%%%% Current Points value: %%%%" + values.size());

//        for (Iterator<Map.Entry<Integer, VectorPoint>> it = currentPoints.entrySet().iterator(); it.hasNext(); ) {
//            Map.Entry<Integer, VectorPoint> entry = it.next();
//            VectorPoint v = entry.getValue();
//            String sb = v.serialize();
//            LOG.fine("%%%% Vector Point: %%%%" + sb.trim());
//            vectorsList.add(v);
//        }
//        LOG.info("vector list size:" + vectorsList.size());
        //vectorsList.add(String.valueOf(content.getContent()));
        DistanceCalculator distanceCalculator = new DistanceCalculator(values, vectorDirectory,
                distanceDirectory, distanceType);
        distanceCalculator.process();
        //distanceCalculator.process(true);
        /*DistanceCalculator distanceCalculator = new DistanceCalculator(currentPoints, distanceDirectory, distanceType);
        distanceCalculator.process();*/
        return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
        super.prepare(cfg, context);
    }

    @Override
    public DataPartition<?> get() {
        return null;
    }
}