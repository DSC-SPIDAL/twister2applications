package edu.iu.dsc.tws.apps.utils;

import edu.iu.dsc.tws.apps.Constants;
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
