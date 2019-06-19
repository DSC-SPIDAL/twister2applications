package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.apps.stockanalysis.utils.CleanMetric;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Utils;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.task.api.BaseSource;

import java.io.File;
import java.util.*;
import java.util.logging.Logger;

public class DataPreProcessingSourceTask  extends BaseSource  {
    private static final Logger LOG = Logger.getLogger(DataPreProcessingSourceTask.class.getName());

    private String dataInputFile;
    private String vectorDirectory;
    private int numberOfDays;
    private Date startDate;
    private Date endDate;
    private int mode;

    private TreeMap<String, List<Date>> dates = new TreeMap<String, List<Date>>();
    private Map<String, CleanMetric> metrics = new HashMap<String, CleanMetric>();

    public DataPreProcessingSourceTask(String datainputfile, String vectordirectory, String numberofdays,
                                       String startdate, String enddate, String mode) {
        this.dataInputFile = datainputfile;
        this.vectorDirectory = vectordirectory;
        this.numberOfDays = Integer.parseInt(numberofdays);
        this.startDate = Utils.parseDateString(startdate);
        this.endDate = Utils.parseDateString(enddate);
        this.mode = Integer.parseInt(mode);
    }

    @Override
    public void execute() {
        LOG.info("I am executing the task");
        PSVectorGenerator psVectorGenerator = new PSVectorGenerator(dataInputFile, vectorDirectory, numberOfDays,
                startDate, endDate, mode);
        Map<Integer, VectorPoint> currentPoints =  psVectorGenerator.process();
        LOG.info("Current Points Values:" + currentPoints.size() + "\t" + currentPoints);
        process();
        context.write(Context.TWISTER2_DIRECT_EDGE, "DataProcessing");
        //startDate = startDate + 1;
        //endDate = endDate + 1;
    }

    //TODO: Move the processing part here (Testing)
    public void process() {
        File inFolder = new File(this.dataInputFile);
        TreeMap<String, List<Date>> allDates = Utils.genDates(startDate, endDate, mode);
        for (String dateString : allDates.keySet()) {
            LOG.info(dateString + " ");
        }
        // create the out directory
        Utils.createDirectory(vectorDirectory);
        this.dates = allDates;
    }

    //TODO: divide the task based on the task parallelism
}