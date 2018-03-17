package edu.iu.dsc.tws.apps.kmeans.utils;

import edu.iu.dsc.tws.apps.kmeans.Constants;
import edu.iu.dsc.tws.common.config.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class JobParameters {
  private static final Logger LOG = Logger.getLogger(JobParameters.class.getName());

  private int size;

  private int iterations;

  private int col;

  private int containers;

  private List<Integer> taskStages;

  private int gap;

  private String fileName;

  private int outstanding = 0;

  private boolean threads = false;

  private int printInterval = 0;

  private String dataType;

  private int dimension;

  private int k;

  private int numPoints;

  private int numCenters;

  private String pointFile;

  private String cenerFile;

  public JobParameters(int size, int iterations, int col,
                       int containers, List<Integer> taskStages, int gap) {
    this.size = size;
    this.iterations = iterations;
    this.col = col;
    this.containers = containers;
    this.taskStages = taskStages;
    this.gap = gap;
  }

  public int getSize() {
    return size;
  }

  public int getIterations() {
    return iterations;
  }

  public int getCol() {
    return col;
  }

  public int getContainers() {
    return containers;
  }

  public List<Integer> getTaskStages() {
    return taskStages;
  }

  public int getGap() {
    return gap;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getFileName() {
    return fileName;
  }

  public int getOutstanding() {
    return outstanding;
  }

  public void setOutstanding(int outstanding) {
    this.outstanding = outstanding;
  }

  public void setThreads(boolean threads) {
    this.threads = threads;
  }

  public boolean isThreads() {
    return threads;
  }

  public int getPrintInterval() {
    return printInterval;
  }

  public String getDataType() {
    return dataType;
  }

  public int getDimension() {
    return dimension;
  }

  public int getK() {
    return k;
  }

  public int getNumPoints() {
    return numPoints;
  }

  public int getNumCenters() {
    return numCenters;
  }

  public String getPointFile() {
    return pointFile;
  }

  public String getCenerFile() {
    return cenerFile;
  }

  public static JobParameters build(Config cfg) {
    int iterations = Integer.parseInt(cfg.getStringValue(Constants.ARGS_ITR));
    int size = Integer.parseInt(cfg.getStringValue(Constants.ARGS_SIZE));
    int col = Integer.parseInt(cfg.getStringValue(Constants.ARGS_COL));
    int containers = Integer.parseInt(cfg.getStringValue(Constants.ARGS_CONTAINERS));
    String taskStages = cfg.getStringValue(Constants.ARGS_TASK_STAGES);
    int gap = Integer.parseInt(cfg.getStringValue(Constants.ARGS_GAP));
    String fName = cfg.getStringValue(Constants.ARGS_FNAME);
    int outstanding = Integer.parseInt(cfg.getStringValue(Constants.ARGS_OUTSTANDING));
    Boolean threads = Boolean.parseBoolean(cfg.getStringValue(Constants.ARGS_THREADS));
    int pi = Integer.parseInt(cfg.getStringValue(Constants.ARGS_PRINT_INTERVAL));
    String type = cfg.getStringValue(Constants.ARGS_DATA_TYPE);

    String pointFile = cfg.getStringValue(Constants.ARGS_POINT);
    String centerFile = cfg.getStringValue(Constants.ARGS_CENTERS);
    int points = Integer.parseInt(cfg.getStringValue(Constants.ARGS_N_POINTS));
    int centers = Integer.parseInt(cfg.getStringValue(Constants.ARGS_N_CENTERS));
    int k = Integer.parseInt(cfg.getStringValue(Constants.ARGS_K));
    int d = Integer.parseInt(cfg.getStringValue(Constants.ARGS_DIMENSIONS));

    String[] stages = taskStages.split(",");
    List<Integer> taskList = new ArrayList<>();
    for (String s : stages) {
      taskList.add(Integer.valueOf(s));
    }

    LOG.info(String.format("Starting with arguments: iter %d size %d col %d containers %d taskStages %s gap %d file %s outstanding %d threads %b",
        iterations, size, col, containers, taskList, gap, fName, outstanding, threads));

    JobParameters jobParameters = new JobParameters(size, iterations, col, containers, taskList, gap);
    jobParameters.fileName = fName;
    jobParameters.outstanding = outstanding;
    jobParameters.threads = threads;
    jobParameters.printInterval = pi;
    jobParameters.dataType = type;
    jobParameters.k = k;
    jobParameters.pointFile = pointFile;
    jobParameters.cenerFile = centerFile;
    jobParameters.numPoints = points;
    jobParameters.numCenters = centers;
    jobParameters.dimension = d;

    return jobParameters;
  }

  @Override
  public String toString() {
    return "JobParameters{" +
        "size=" + size +
        ", iterations=" + iterations +
        ", col=" + col +
        ", containers=" + containers +
        '}';
  }
}
