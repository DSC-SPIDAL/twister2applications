package edu.iu.dsc.tws.apps.kmeans;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.apps.kmeans.utils.Utils;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.logging.Logger;

public class Program {
  private static final Logger LOG = Logger.getLogger(Program.class.getName());
  public static void main(String[] args) throws ParseException {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    Options options = new Options();
    options.addOption(Constants.ARGS_CONTAINERS, true, "Containers");
    options.addOption(Constants.ARGS_SIZE, true, "Size");
    options.addOption(Constants.ARGS_ITR, true, "Iteration");
    options.addOption(Utils.createOption(Constants.ARGS_COL, true, "Cols", true));
    options.addOption(Utils.createOption(Constants.ARGS_TASK_STAGES, true, "Throughput mode", true));
    options.addOption(Utils.createOption(Constants.ARGS_GAP, true, "Gap", false));
    options.addOption(Utils.createOption(Constants.ARGS_FNAME, true, "File name", true));
    options.addOption(Utils.createOption(Constants.ARGS_OUTSTANDING, true, "Throughput no of messages", false));
    options.addOption(Utils.createOption(Constants.ARGS_THREADS, true, "Threads", false));
    options.addOption(Utils.createOption(Constants.ARGS_PRINT_INTERVAL, true, "Threads", false));
    options.addOption(Utils.createOption(Constants.ARGS_DATA_TYPE, true, "Data", false));
    options.addOption(Utils.createOption(Constants.ARGS_POINT, true, "Points", true));
    options.addOption(Utils.createOption(Constants.ARGS_CENTERS, true, "Centers", true));
    options.addOption(Utils.createOption(Constants.ARGS_DIMENSIONS, true, "Dimensions", true));
    options.addOption(Utils.createOption(Constants.ARGS_K, true, "K", true));
    options.addOption(Utils.createOption(Constants.ARGS_N_POINTS, true, "K", true));

    CommandLineParser commandLineParser = new BasicParser();
    CommandLine cmd = commandLineParser.parse(options, args);
    int containers = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_CONTAINERS));
    int size = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_SIZE));
    int itr = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_ITR));
    int col = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_COL));
    String point = cmd.getOptionValue(Constants.ARGS_POINT);
    String centersFile = cmd.getOptionValue(Constants.ARGS_CENTERS);
    String k = cmd.getOptionValue(Constants.ARGS_K);
    String np = cmd.getOptionValue(Constants.ARGS_N_POINTS);
    String dim = cmd.getOptionValue(Constants.ARGS_DIMENSIONS);

    String threads = "true";
    if (cmd.hasOption(Constants.ARGS_THREADS)) {
      threads = cmd.getOptionValue(Constants.ARGS_THREADS);
    }
    String taskStages = cmd.getOptionValue(Constants.ARGS_TASK_STAGES);
    String gap = "0";
    if (cmd.hasOption(Constants.ARGS_GAP)) {
      gap = cmd.getOptionValue(Constants.ARGS_GAP);
    }
    String fName = "";
    if (cmd.hasOption(Constants.ARGS_FNAME)) {
      fName = cmd.getOptionValue(Constants.ARGS_FNAME);
    }
    String outstanding = "0";
    if (cmd.hasOption(Constants.ARGS_OUTSTANDING)) {
      outstanding = cmd.getOptionValue(Constants.ARGS_OUTSTANDING);
    }
    String printInt = "0";
    if (cmd.hasOption(Constants.ARGS_PRINT_INTERVAL)) {
      printInt = cmd.getOptionValue(Constants.ARGS_PRINT_INTERVAL);
    }
    String dataType = "default";
    if (cmd.hasOption(Constants.ARGS_DATA_TYPE)) {
      dataType = cmd.getOptionValue(Constants.ARGS_DATA_TYPE);
    }

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put(Constants.ARGS_ITR, Integer.toString(itr));
    jobConfig.put(Constants.ARGS_COL, Integer.toString(col));
    jobConfig.put(Constants.ARGS_SIZE, Integer.toString(size));
    jobConfig.put(Constants.ARGS_CONTAINERS, Integer.toString(containers));
    jobConfig.put(Constants.ARGS_TASK_STAGES, taskStages);
    jobConfig.put(Constants.ARGS_GAP, gap);
    jobConfig.put(Constants.ARGS_FNAME, fName);
    jobConfig.put(Constants.ARGS_OUTSTANDING, outstanding);
    jobConfig.put(Constants.ARGS_THREADS, threads);
    jobConfig.put(Constants.ARGS_PRINT_INTERVAL, printInt);
    jobConfig.put(Constants.ARGS_DATA_TYPE, dataType);
    jobConfig.put(Constants.ARGS_CENTERS, centersFile);
    jobConfig.put(Constants.ARGS_N_POINTS, np);
    jobConfig.put(Constants.ARGS_K, k);
    jobConfig.put(Constants.ARGS_POINT, point);
    jobConfig.put(Constants.ARGS_DIMENSIONS, dim);

    // build the job
    BasicJob basicJob = null;
    basicJob = BasicJob.newBuilder()
        .setName("kmeans-bench")
        .setContainerClass(KMeans2.class.getName())
        .setRequestResource(new ResourceContainer(2, 1024), containers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitContainerJob(basicJob, config);
  }
}
