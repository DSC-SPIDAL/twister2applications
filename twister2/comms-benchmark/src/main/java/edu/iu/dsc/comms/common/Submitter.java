package edu.iu.dsc.comms.common;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;

public class Submitter {

    public static void submitJob(Config config, int containers, JobConfig jobConfig, String clazz) {
        Twister2Job twister2Job;
        twister2Job = Twister2Job.newBuilder()
                .setJobName(clazz)
                .setWorkerClass(clazz)
                .addComputeResource(1, 512, containers)
                .setConfig(jobConfig)
                .build();
        // now submit the job
        Twister2Submitter.submitJob(twister2Job, config);
    }
}
