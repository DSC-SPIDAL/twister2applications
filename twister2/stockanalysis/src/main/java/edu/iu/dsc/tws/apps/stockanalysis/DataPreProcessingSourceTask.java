package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.task.api.BaseSource;

import java.util.Map;
import java.util.logging.Logger;

public class DataPreProcessingSourceTask  extends BaseSource implements Receptor {
    private static final Logger LOG = Logger.getLogger(DataPreProcessingSourceTask.class.getName());

    private String dataInputFile;
    private String vectorDirectory;
    private String numberOfDays;
    private String startDate;
    private String endDate;
    private String mode;

    public DataPreProcessingSourceTask(String datainputfile, String vectordirectory, String numberofdays,
                                       String startdate, String endDate, String mode) {
        this.dataInputFile = datainputfile;
        this.vectorDirectory = vectordirectory;
        this.numberOfDays = numberofdays;
        this.startDate = startdate;
        this.endDate = endDate;
        this.mode = mode;
    }

    @Override
    public void execute() {
        LOG.info("I am executing the task");
        PSVectorGenerator psVectorGenerator = new PSVectorGenerator(dataInputFile, vectorDirectory,
                Integer.parseInt(numberOfDays), startDate, endDate, Integer.parseInt(mode));
        Map<Integer, VectorPoint> currentPoints =  psVectorGenerator.process();
        LOG.info("Current Points Values:" + currentPoints.size() + "\t" + currentPoints);
        context.write(Context.TWISTER2_DIRECT_EDGE, "Stock Analysis Execution");
        //startDate = startDate + 1;
        //endDate = endDate + 1;
    }

    @Override
    public void add(String name, DataObject<?> data) {
    }
}