package edu.iu.dsc.tws.apps.utils;

import edu.iu.dsc.tws.apps.Constants;
import edu.iu.dsc.tws.common.config.Config;

public class JobParameters {
  private int size;

  private int iterations;

  private int col;

  private int parallel;

  private int containers;

  private int tasks;

  public JobParameters(int size, int iterations, int col, int parallel, int containers, int tasks) {
    this.size = size;
    this.iterations = iterations;
    this.col = col;
    this.parallel = parallel;
    this.containers = containers;
    this.tasks = tasks;
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

  public int getParallel() {
    return parallel;
  }

  public int getContainers() {
    return containers;
  }

  public int getTasks() {
    return tasks;
  }

  public static JobParameters build(Config cfg) {
    int iterations = Integer.parseInt(cfg.getStringValue(Constants.ARGS_ITR));
    int size = Integer.parseInt(cfg.getStringValue(Constants.ARGS_SIZE));
    int col = Integer.parseInt(cfg.getStringValue(Constants.ARGS_COL));
    int parallel = Integer.parseInt(cfg.getStringValue(Constants.ARGS_PARALLEL));
    int containers = Integer.parseInt(cfg.getStringValue(Constants.ARGS_CONTAINERS));
    int tasks = Integer.parseInt(cfg.getStringValue(Constants.ARGS_CONTAINERS));
    return new JobParameters(size, iterations, col, parallel, containers, tasks);
  }
}
