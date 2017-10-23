package edu.iu.dsc.tws.mpiapps;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.InputFormat;
import edu.iu.dsc.tws.data.api.formatters.TextInputFormatter;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;
import edu.iu.dsc.tws.mpiapps.configuration.ConfigurationMgr;
import edu.iu.dsc.tws.mpiapps.configuration.section.SmartHomeSection;
import org.apache.commons.cli.*;

import edu.iu.dsc.tws.data.*;
import com.google.common.base.Optional;

import java.nio.ByteOrder;

/**
 * Created by pulasthi on 10/18/17.
 */
public class SmartHomesDriver {
    private static Options programOptions = new Options();
    static {
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_C),
                Constants.CMD_OPTION_LONG_C, true,
                Constants.CMD_OPTION_DESCRIPTION_C);
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_N),
                Constants.CMD_OPTION_LONG_N, true,
                Constants.CMD_OPTION_DESCRIPTION_N);
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_T),
                Constants.CMD_OPTION_LONG_T, true,
                Constants.CMD_OPTION_DESCRIPTION_T);

        programOptions.addOption(Constants.CMD_OPTION_SHORT_MMAPS, true, Constants.CMD_OPTION_DESCRIPTION_MMAPS);
        programOptions.addOption(
                Constants.CMD_OPTION_SHORT_MMAP_SCRATCH_DIR, true,
                Constants.CMD_OPTION_DESCRIPTION_MMAP_SCRATCH_DIR);

        programOptions.addOption(
                Constants.CMD_OPTION_SHORT_BIND_THREADS, true,
                Constants.CMD_OPTION_DESCRIPTION_BIND_THREADS);

        programOptions.addOption(
                Constants.CMD_OPTION_SHORT_CPS, true,
                Constants.CMD_OPTION_DESCRIPTION_CPS);
    }
    public static SmartHomeSection config;

    public static void main(String[] args) {
        Optional<CommandLine> parserResult =
                parseCommandLineArguments(args, programOptions);

        if (!parserResult.isPresent()) {
            System.out.println(Constants.ERR_PROGRAM_ARGUMENTS_PARSING_FAILED);
            new HelpFormatter()
                    .printHelp(Constants.PROGRAM_NAME, programOptions);
            return;
        }

        CommandLine cmd = parserResult.get();
        if (!(cmd.hasOption(Constants.CMD_OPTION_LONG_C) &&
                cmd.hasOption(Constants.CMD_OPTION_LONG_N) &&
                cmd.hasOption(Constants.CMD_OPTION_LONG_T))) {
            System.out.println(Constants.ERR_INVALID_PROGRAM_ARGUMENTS);
            new HelpFormatter()
                    .printHelp(Constants.PROGRAM_NAME, programOptions);
            return;
        }

        readConfiguration(cmd);
        //Initial data read to setup parallism
        // read data using twsiter2 data api

        Config.Builder builder = new Config.Builder();
        builder.put("input.file.path",config.dataFile);
        Config txtFileConf = builder.build();
        Path path = new Path(config.dataFile);
        InputFormat txtInput = new TextInputFormatter(path);
        txtInput.configure(txtFileConf);
        int minSplits = 1;

        try {
            InputSplit[] inputSplits = txtInput.createInputSplits(minSplits);
            InputSplitAssigner inputSplitAssigner = txtInput.getInputSplitAssigner(inputSplits);
            InputSplit cur = inputSplitAssigner.getNextInputSplit(null,0);
            txtInput.open(cur);
            String line = "";
            line = (String)txtInput.nextRecord(line);
            System.out.println(line);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void readConfiguration(CommandLine cmd) {
        config = ConfigurationMgr.LoadConfiguration(
                cmd.getOptionValue(Constants.CMD_OPTION_LONG_C)).smartHomesSection;
        ParallelOps.nodeCount =
                Integer.parseInt(cmd.getOptionValue(Constants.CMD_OPTION_LONG_N));
        ParallelOps.threadCount =
                Integer.parseInt(cmd.getOptionValue(Constants.CMD_OPTION_LONG_T));
    }

    /**
     * Parse command line arguments
     *
     * @param args Command line arguments
     * @param opts Command line options
     * @return An <code>Optional&lt;CommandLine&gt;</code> object
     */
    private static Optional<CommandLine> parseCommandLineArguments(
            String[] args, Options opts) {

        CommandLineParser optParser = new GnuParser();

        try {
            return Optional.fromNullable(optParser.parse(opts, args));
        }
        catch (ParseException e) {
            e.printStackTrace();
        }
        return Optional.fromNullable(null);
    }
}
