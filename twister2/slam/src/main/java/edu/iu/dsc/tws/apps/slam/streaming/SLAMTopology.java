package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class SLAMTopology {
    private static Logger LOG = LoggerFactory.getLogger(SLAMTopology.class);

    public static void main(String[] args) throws Exception {
        // first load the configurations from command line and config files
        Config config = ResourceAllocator.loadConfig(new HashMap<>());
        Options options = new Options();
        options.addOption(Constants.ARGS_PARALLEL, true, "No of parallel nodes");
        options.addOption(Constants.ARGS_PARTICLES, true, "No of particles");
        options.addOption(Constants.CONFIG_FILE, true, "Config file");
        options.addOption(Constants.INPUT_FILE, true, "Input file");

        CommandLineParser commandLineParser = new BasicParser();
        CommandLine cmd = commandLineParser.parse(options, args);
        String pValue = cmd.getOptionValue(Constants.ARGS_PARALLEL);
        String particles = cmd.getOptionValue(Constants.ARGS_PARTICLES);
        String configFile = cmd.getOptionValue(Constants.CONFIG_FILE);
        String inputFile = cmd.getOptionValue(Constants.INPUT_FILE);
        int p = Integer.parseInt(pValue);

        // build JobConfig
        JobConfig jobConfig = new JobConfig();
        jobConfig.put(Constants.ARGS_PARTICLES, particles);
        jobConfig.put(Constants.ARGS_PARALLEL, pValue);
        jobConfig.put(Constants.CONFIG_FILE, configFile);
        jobConfig.put(Constants.INPUT_FILE, inputFile);

        // build the job
        BasicJob basicJob = null;
        basicJob = BasicJob.newBuilder()
            .setName("kmeans-bench")
            .setContainerClass(SlamWorker.class.getName())
            .setRequestResource(new ResourceContainer(2.0, 1024), p + 1)
            .setConfig(jobConfig)
            .build();
        // now submit the job
        Twister2Submitter.submitContainerJob(basicJob, config);
    }
}
