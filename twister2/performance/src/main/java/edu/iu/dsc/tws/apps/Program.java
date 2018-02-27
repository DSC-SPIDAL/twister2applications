package edu.iu.dsc.tws.apps;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.apps.batch.AllReduce;
import edu.iu.dsc.tws.apps.batch.MultiGather;
import edu.iu.dsc.tws.apps.batch.MultiReduce;
import edu.iu.dsc.tws.apps.batch.Reduce;
import edu.iu.dsc.tws.apps.stream.*;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;

import java.util.HashMap;

public class Program {
  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    int containers = Integer.parseInt(args[0]);
    int size = Integer.parseInt(args[1]);
    int itr = Integer.parseInt(args[2]);
    int col = Integer.parseInt(args[3]);
    boolean stream = Boolean.parseBoolean(args[4]);
    String taskStages = args[5];
    String gap = args[6];

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put(Constants.ARGS_ITR, Integer.toString(itr));
    jobConfig.put(Constants.ARGS_COL, Integer.toString(col));
    jobConfig.put(Constants.ARGS_SIZE, Integer.toString(size));
    jobConfig.put(Constants.ARGS_CONTAINERS, Integer.toString(containers));
    jobConfig.put(Constants.ARGS_TASK_STAGES, taskStages);
    jobConfig.put(Constants.ARGS_GAP, gap);

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
      }
    }
  }
}
