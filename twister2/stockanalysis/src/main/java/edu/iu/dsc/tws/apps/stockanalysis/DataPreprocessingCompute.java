package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.nodes.BaseCompute;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;

import java.util.*;
import java.util.logging.Logger;


public class DataPreprocessingCompute extends BaseCompute {

    private static final Logger LOG = Logger.getLogger(DataPreprocessingCompute.class.getName());

    private String vectorDirectory;
    private String distanceDirectory;
    private int distanceType;

    private String edgeName;
    private List<Map<Integer, VectorPoint>> values;

    public DataPreprocessingCompute(String vectordirectory, String distancedirectory, int distancetype,
                                    String edgename) {
        this.vectorDirectory = vectordirectory;
        this.distanceDirectory = distancedirectory;
        this.distanceType = distancetype;
        this.edgeName = edgename;
    }

    @Override
    public boolean execute(IMessage content) {
        values = new ArrayList<>();
        values.add((Map<Integer, VectorPoint>) content.getContent());
        context.write(edgeName, values);
        return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
        super.prepare(cfg, context);
    }
}
