package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.nodes.BaseCompute;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class DataPreprocessingComputeTask extends BaseCompute {

    private static final Logger LOG = Logger.getLogger(DataPreprocessingComputeTask.class.getName());

    private String vectorDirectory;
    private String distanceDirectory;
    private int distanceType;

    private String edgeName;
    private List<Map<Integer, VectorPoint>> values;
    private List<String> vectorPoints;

    public DataPreprocessingComputeTask(String vectordirectory, String distancedirectory,
                                    int distancetype, String edgename) {
        this.vectorDirectory = vectordirectory;
        this.distanceDirectory = distancedirectory;
        this.distanceType = distancetype;
        this.edgeName = edgename;
    }

    @Override
    public boolean execute(IMessage content) {
        /*values = new ArrayList<>();
        if (content.getContent() != null) {
            values.add((Map<Integer, VectorPoint>) content.getContent());
        } else {
            LOG.info("Content values are null");
        }
        if (!values.isEmpty()) {
            context.write(edgeName, values);
        }*/

        vectorPoints = new ArrayList<>();
        if (content.getContent() != null) {
            vectorPoints.add(String.valueOf(content.getContent()));
        }
        LOG.info("vector points:" + vectorPoints.size());
        context.write(edgeName, vectorPoints);
        return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
        super.prepare(cfg, context);
    }
}
