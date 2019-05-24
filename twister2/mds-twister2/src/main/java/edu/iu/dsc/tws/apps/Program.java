package edu.iu.dsc.tws.apps;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.apps.mds.MDSWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.logging.Logger;

public class Program {
    private static final Logger LOG = Logger.getLogger(Program.class.getName());
    public static void main(String[] args) throws ParseException {
        // first load the configurations from command line and config files
        Config config = ResourceAllocator.loadConfig(new HashMap<>());

        // build JobConfig
        HashMap<String, Object> configurations = new HashMap<>();
        configurations.put(SchedulerContext.THREADS_PER_WORKER, 1);

        Options options = new Options();
        options.addOption(DataObjectConstants.WORKERS, true, "Workers");
        options.addOption(DataObjectConstants.PARALLELISM_VALUE, true, "parallelism");

        options.addOption(DataObjectConstants.DSIZE, true, "Size of the matrix rows");
        options.addOption(DataObjectConstants.DIMENSIONS, true, "dimension of the matrix");
        options.addOption(DataObjectConstants.BYTE_TYPE, true, "bytetype");

        options.addOption(Utils.createOption(DataObjectConstants.DINPUT_DIRECTORY,
                true, "Matrix Input Creation directory", true));
        options.addOption(Utils.createOption(DataObjectConstants.FILE_SYSTEM,
                true, "file system", true));

        @SuppressWarnings("deprecation")
        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine cmd = commandLineParser.parse(options, args);

        int workers = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.WORKERS));
        int dsize = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.DSIZE));

        int dimension = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.DIMENSIONS));
        int parallelismValue = Integer.parseInt(cmd.getOptionValue(
                DataObjectConstants.PARALLELISM_VALUE));

        String byteType = cmd.getOptionValue(DataObjectConstants.BYTE_TYPE);
        String dataDirectory = cmd.getOptionValue(DataObjectConstants.DINPUT_DIRECTORY);
        String fileSystem = cmd.getOptionValue(DataObjectConstants.FILE_SYSTEM);

        // build JobConfig
        JobConfig jobConfig = new JobConfig();
        jobConfig.put(DataObjectConstants.WORKERS, Integer.toString(workers));
        jobConfig.put(DataObjectConstants.PARALLELISM_VALUE, Integer.toString(parallelismValue));

        jobConfig.put(DataObjectConstants.DIMENSIONS, Integer.toString(dimension));
        jobConfig.put(DataObjectConstants.DSIZE, Integer.toString(dsize));

        jobConfig.put(DataObjectConstants.BYTE_TYPE, byteType);
        jobConfig.put(DataObjectConstants.DINPUT_DIRECTORY, dataDirectory);
        jobConfig.put(DataObjectConstants.FILE_SYSTEM, fileSystem);

        // build JobConfig
        Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
        jobBuilder.setJobName("MatrixGenerator-job");
        jobBuilder.setWorkerClass(MDSWorker.class.getName());
        jobBuilder.addComputeResource(2, 512, 1.0, workers);
        jobBuilder.setConfig(jobConfig);

        // now submit the job
        Twister2Submitter.submitJob(jobBuilder.build(), config);

    }
}
