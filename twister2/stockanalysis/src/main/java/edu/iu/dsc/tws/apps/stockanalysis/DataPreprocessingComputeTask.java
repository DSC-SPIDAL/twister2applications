package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.nodes.BaseCompute;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class DataPreprocessingComputeTask extends BaseCompute {

    private static final Logger LOG = Logger.getLogger(DataPreprocessingComputeTask.class.getName());

    private String vectorDirectory;
    private String distanceDirectory;
    private int distanceType;

    private String edgeName;
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
        LOG.fine("message content:" + content.getContent());
        vectorPoints = new ArrayList<>();
        if (content.getContent() != null) {
            vectorPoints.add(String.valueOf(content.getContent()));
        }
        context.write(edgeName, vectorPoints);
        return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
        super.prepare(cfg, context);
    }
}
