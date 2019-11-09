package edu.iu.dsc.tws.apps.mds;

import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;

import java.util.logging.Logger;

public class DataGeneratorSource extends BaseSource {

    private static final Logger LOG = Logger.getLogger(DataGeneratorSource.class.getName());

    private String edge;
    private int dinputSize;
    private int dim;
    private String directory;
    private String byteType;

    public DataGeneratorSource() {
    }
    public DataGeneratorSource(String twister2DirectEdge, int dsize,
                               int dimension, String dataDirectory, String bytetype) {
        this.edge = twister2DirectEdge;
        this.dinputSize = dsize;
        this.dim = dimension;
        this.directory = dataDirectory;
        this.byteType = bytetype;
    }

    @Override
    public void execute() {
        generateData(config);
        context.writeEnd(edge, "Data Generation Finished");
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
        super.prepare(cfg, context);
    }

    private void generateData(Config config) {
        MatrixGenerator matrixGen = new MatrixGenerator(config);
        matrixGen.generate(dinputSize, dim, directory, byteType);
    }
}
