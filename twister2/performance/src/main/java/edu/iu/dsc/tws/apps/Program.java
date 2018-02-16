package edu.iu.dsc.tws.apps;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.apps.stream.Reduce;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;

import java.util.HashMap;

public class Program {
  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    int parallel = Integer.parseInt(args[0]);
    int containers = Integer.parseInt(args[1]);
    int size = Integer.parseInt(args[2]);
    int itr = Integer.parseInt(args[3]);
    int col = Integer.parseInt(args[4]);
    boolean stream = Boolean.parseBoolean(args[5]);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put(Constants.ARGS_ITR, Integer.toString(itr));
    jobConfig.put(Constants.ARGS_COL, Integer.toString(col));
    jobConfig.put(Constants.ARGS_SIZE, Integer.toString(size));
    jobConfig.put(Constants.ARGS_PARALLEL, Integer.toString(parallel));
    jobConfig.put(Constants.ARGS_CONTAINERS, Integer.toString(containers));

    // build the job
    BasicJob basicJob = null;
    if (!stream) {
      if (col == 0) {
        basicJob = BasicJob.newBuilder()
            .setName("basic-hl-reduce")
            .setContainerClass(Reduce.class.getName())
            .setRequestResource(new ResourceContainer(2, 1024), 4)
            .setConfig(jobConfig)
            .build();
      } else if (col == 1) {

      }
    }
    // now submit the job
    Twister2Submitter.submitContainerJob(basicJob, config);
  }
}
