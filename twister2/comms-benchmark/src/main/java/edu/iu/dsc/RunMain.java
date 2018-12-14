package edu.iu.dsc;

import edu.iu.dsc.comms.CommsRunner;
import edu.iu.dsc.comms.akeyed.AKeyedRunner;
import edu.iu.dsc.comms.akeyed.examples.BPartitionExample;
import edu.iu.dsc.constant.Constants;
import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.util.Utils;
import org.apache.commons.cli.*;

import java.util.HashMap;

public class RunMain {
    //This class runs the examples depending on user arguments
    /*
    TODO: First define userinput arguments so that we can run the examples in an abstract manner.
    Example : ./tws-comm -example partition -type keyed ... general tws init-args
    * */
    public static void main(String[] args) throws ParseException {
        // first load the configurations from command line and config files
        Config config = ResourceAllocator.loadConfig(new HashMap<>());

        Options options = new Options();
        options.addOption(Constants.ARGS_PARALLELISM, true, "parallelism");
        options.addOption(Constants.ARGS_SIZE, true, "Size");
        options.addOption(Constants.ARGS_ITR, true, "Iteration");
        options.addOption(Utils.createOption(Constants.ARGS_OPERATION, true, "Operation", true));
        options.addOption(Constants.ARGS_STREAM, false, "Stream");
        options.addOption(Utils.createOption(Constants.ARGS_TASK_STAGES, true, "Throughput mode", true));
        options.addOption(Utils.createOption(Constants.ARGS_GAP, true, "Gap", false));
        options.addOption(Utils.createOption(Constants.ARGS_FNAME, true, "File name", false));
        options.addOption(Utils.createOption(Constants.ARGS_APP_NAME, true, "App Name", false));
        options.addOption(Utils.createOption(Constants.ARGS_OUTSTANDING, true, "Throughput no of messages", false));
        options.addOption(Utils.createOption(Constants.ARGS_THREADS, true, "Threads", false));
        options.addOption(Utils.createOption(Constants.ARGS_PRINT_INTERVAL, true, "Threads", false));
        options.addOption(Utils.createOption(Constants.ARGS_DATA_TYPE, true, "Data", false));
        options.addOption(Utils.createOption(Constants.ARGS_KEYED, true, "Operation Type , ex : keyed", false));
        options.addOption(Utils.createOption(Constants.ARGS_INIT_ITERATIONS, true, "Data", false));
        options.addOption(Constants.ARGS_VERIFY, false, "verify");
        options.addOption(Constants.ARGS_COMMS, false, "comms");
        options.addOption(Constants.ARGS_TASK_EXEC, false, "taske");
        options.addOption(Constants.ARGS_APP, false, "app");

        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine cmd = commandLineParser.parse(options, args);
        int parallelism = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_PARALLELISM));
        int size = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_SIZE));
        int itr = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_ITR));
        String operation = cmd.getOptionValue(Constants.ARGS_OPERATION);
        boolean stream = cmd.hasOption(Constants.ARGS_STREAM);
        boolean verify = cmd.hasOption(Constants.ARGS_VERIFY);
        boolean keyed = cmd.hasOption(Constants.ARGS_KEYED);
        boolean taskExec = cmd.hasOption(Constants.ARGS_TASK_EXEC);
        boolean app = cmd.hasOption(Constants.ARGS_APP);
        boolean comms = cmd.hasOption(Constants.ARGS_COMMS);

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

        String printInt = "1";
        if (cmd.hasOption(Constants.ARGS_PRINT_INTERVAL)) {
            printInt = cmd.getOptionValue(Constants.ARGS_PRINT_INTERVAL);
        }

        String dataType = "default";
        if (cmd.hasOption(Constants.ARGS_DATA_TYPE)) {
            dataType = cmd.getOptionValue(Constants.ARGS_DATA_TYPE);
        }
        String intItr = "0";
        if (cmd.hasOption(Constants.ARGS_INIT_ITERATIONS)) {
            intItr = cmd.getOptionValue(Constants.ARGS_INIT_ITERATIONS);
        }

        // build JobConfig
        JobConfig jobConfig = new JobConfig();
        jobConfig.put(Constants.ARGS_ITR, Integer.toString(itr));
        jobConfig.put(Constants.ARGS_OPERATION, operation);
        jobConfig.put(Constants.ARGS_SIZE, Integer.toString(size));
        jobConfig.put(Constants.ARGS_PARALLELISM, Integer.toString(parallelism));
        jobConfig.put(Constants.ARGS_TASK_STAGES, taskStages);
        jobConfig.put(Constants.ARGS_GAP, gap);
        jobConfig.put(Constants.ARGS_FNAME, fName);
        jobConfig.put(Constants.ARGS_OUTSTANDING, outstanding);
        jobConfig.put(Constants.ARGS_THREADS, threads);
        jobConfig.put(Constants.ARGS_PRINT_INTERVAL, printInt);
        jobConfig.put(Constants.ARGS_DATA_TYPE, dataType);
        jobConfig.put(Constants.ARGS_INIT_ITERATIONS, intItr);
        jobConfig.put(Constants.ARGS_VERIFY, verify);
        jobConfig.put(Constants.ARGS_STREAM, stream);
        jobConfig.put(Constants.ARGS_KEYED, keyed);
        jobConfig.put(Constants.ARGS_TASK_EXEC, taskExec);
        jobConfig.put(Constants.ARGS_STREAM, app);
        jobConfig.put(Constants.ARGS_COMMS, comms);

        // build the job
        if(!app) {
            switch (operation) {

            }

        } else {

        }

        if(!keyed) {
            if(comms) {
                CommsRunner commsRunner = new CommsRunner(config, jobConfig, stream, operation, parallelism, keyed);
                commsRunner.run();
            }
        }



        if(!keyed) {
            if(taskExec) {

            }
        }


    }




}
