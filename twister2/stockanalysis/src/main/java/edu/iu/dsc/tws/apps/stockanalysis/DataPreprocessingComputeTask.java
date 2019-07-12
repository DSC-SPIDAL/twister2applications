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

    public DataPreprocessingComputeTask(String vectordirectory, String distancedirectory,
                                    int distancetype, String edgename) {
        this.vectorDirectory = vectordirectory;
        this.distanceDirectory = distancedirectory;
        this.distanceType = distancetype;
        this.edgeName = edgename;
    }

    @Override
    public boolean execute(IMessage content) {
        values = new ArrayList<>();
        if (content.getContent() != null) {
            values.add((Map<Integer, VectorPoint>) content.getContent());
            for (Map<Integer, VectorPoint> currentPoints : values) {
                for (Map.Entry<Integer, VectorPoint> entry : currentPoints.entrySet()) {
                    VectorPoint v = entry.getValue();
                    LOG.fine("%%% Serialized Value: %%%" + v.serialize());
                }
            }
        } else {
            LOG.info("Content values are null");
        }
        context.write(edgeName, values);
        return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
        super.prepare(cfg, context);
    }
}
