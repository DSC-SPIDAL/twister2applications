package edu.iu.dsc.tws.apps;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.apps.batch.AllReduce;
import edu.iu.dsc.tws.apps.batch.MultiGather;
import edu.iu.dsc.tws.apps.batch.MultiReduce;
import edu.iu.dsc.tws.apps.batch.Reduce;
import edu.iu.dsc.tws.apps.storm.PartitionStream;
import edu.iu.dsc.tws.apps.stream.*;
import edu.iu.dsc.tws.apps.utils.Utils;
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
    options.addOption(Constants.ARGS_STREAM, false, "Stream");
    options.addOption(Utils.createOption(Constants.ARGS_TASK_STAGES, true, "Throughput mode", true));
    options.addOption(Utils.createOption(Constants.ARGS_GAP, true, "Gap", false));
    options.addOption(Utils.createOption(Constants.ARGS_FNAME, true, "File name", true));
    options.addOption(Utils.createOption(Constants.ARGS_OUTSTANDING, true, "Throughput no of messages", false));
    options.addOption(Utils.createOption(Constants.ARGS_THREADS, false, "Threads", false));
    options.addOption(Utils.createOption(Constants.ARGS_PRINT_INTERVAL, true, "Threads", false));
    options.addOption(Utils.createOption(Constants.ARGS_DATA_TYPE, true, "Data", false));

    CommandLineParser commandLineParser = new BasicParser();
    CommandLine cmd = commandLineParser.parse(options, args);
    int containers = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_CONTAINERS));
    int size = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_SIZE));
    int itr = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_ITR));
    int col = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_COL));
    boolean stream = cmd.hasOption(Constants.ARGS_STREAM);
    boolean threads = cmd.hasOption(Constants.ARGS_THREADS);
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
    jobConfig.put(Constants.ARGS_THREADS, Boolean.toString(threads));
    jobConfig.put(Constants.ARGS_PRINT_INTERVAL, printInt);
    jobConfig.put(Constants.ARGS_DATA_TYPE, dataType);

    // build the job
    BasicJob basicJob = null;
    if (!stream) {
      if (col == 0) {
        basicJob = BasicJob.newBuilder()
            .setName("reduce-bench")
            .setContainerClass(Reduce.class.getName())
            .setRequestResource(new ResourceContainer(2, 1024), containers)
            .setConfig(jobConfig)
            .build();
        // now submit the job
        Twister2Submitter.submitContainerJob(basicJob, config);
      } else if (col == 1) {
        basicJob = BasicJob.newBuilder()
            .setName("all-reduce-bench")
            .setContainerClass(AllReduce.class.getName())
            .setRequestResource(new ResourceContainer(2, 1024), containers)
            .setConfig(jobConfig)
            .build();
        // now submit the job
        Twister2Submitter.submitContainerJob(basicJob, config);
      } else if (col == 3) {
        basicJob = BasicJob.newBuilder()
            .setName("multi-reduce-bench")
            .setContainerClass(MultiReduce.class.getName())
            .setRequestResource(new ResourceContainer(2, 1024), containers)
            .setConfig(jobConfig)
            .build();
        // now submit the job
        Twister2Submitter.submitContainerJob(basicJob, config);
      }  else if (col == 4) {
        basicJob = BasicJob.newBuilder()
            .setName("multi-gather-bench")
            .setContainerClass(MultiGather.class.getName())
            .setRequestResource(new ResourceContainer(2, 1024), containers)
            .setConfig(jobConfig)
            .build();
        // now submit the job
        Twister2Submitter.submitContainerJob(basicJob, config);
      }
    } else {
      if (col == 0) {
        basicJob = BasicJob.newBuilder()
            .setName("reduce-stream-bench")
            .setContainerClass(ReduceStream.class.getName())
            .setRequestResource(new ResourceContainer(2, 1024), containers)
            .setConfig(jobConfig)
            .build();
        // now submit the job
        Twister2Submitter.submitContainerJob(basicJob, config);
      } else if (col == 1) {
        basicJob = BasicJob.newBuilder()
            .setName("all-reduce-stream-bench")
            .setContainerClass(AllReduceStream.class.getName())
            .setRequestResource(new ResourceContainer(2, 1024), containers)
            .setConfig(jobConfig)
            .build();
        // now submit the job
        Twister2Submitter.submitContainerJob(basicJob, config);
      } else if (col == 2) {
        basicJob = BasicJob.newBuilder()
            .setName("all-reduce-stream-bench")
            .setContainerClass(IntAllReduceStream.class.getName())
            .setRequestResource(new ResourceContainer(2, 1024), containers)
            .setConfig(jobConfig)
            .build();
        // now submit the job
        Twister2Submitter.submitContainerJob(basicJob, config);
      } else if (col == 3) {
        basicJob = BasicJob.newBuilder()
            .setName("gather-stream-bench")
            .setContainerClass(GatherStream.class.getName())
            .setRequestResource(new ResourceContainer(2, 1024), containers)
            .setConfig(jobConfig)
            .build();
        // now submit the job
        Twister2Submitter.submitContainerJob(basicJob, config);
      }  else if (col == 4) {
        basicJob = BasicJob.newBuilder()
            .setName("all-gather-stream-bench")
            .setContainerClass(AllGatherStream.class.getName())
            .setRequestResource(new ResourceContainer(2, 1024), containers)
            .setConfig(jobConfig)
            .build();
        // now submit the job
        Twister2Submitter.submitContainerJob(basicJob, config);
      } else if (col == 5) {
        basicJob = BasicJob.newBuilder()
            .setName("partition-stream-bench")
            .setContainerClass(PartitionStream.class.getName())
            .setRequestResource(new ResourceContainer(2, 1024), containers)
            .setConfig(jobConfig)
            .build();
        // now submit the job
        Twister2Submitter.submitContainerJob(basicJob, config);
      } else if (col == 6) {
        basicJob = BasicJob.newBuilder()
            .setName("partition-stream-bench")
            .setContainerClass(edu.iu.dsc.tws.apps.storm.ReduceStream.class.getName())
            .setRequestResource(new ResourceContainer(2, 1024), containers)
            .setConfig(jobConfig)
            .build();
        // now submit the job
        Twister2Submitter.submitContainerJob(basicJob, config);
      }
    }
  }
}
