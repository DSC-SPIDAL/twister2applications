package edu.iu.dsc.tws.mpiapps;

import edu.iu.dsc.tws.mpiapps.configuration.ConfigurationMgr;
import edu.iu.dsc.tws.mpiapps.configuration.section.SmartHomeSection;
import org.apache.commons.cli.*;

import com.google.common.base.Optional;

import java.nio.ByteOrder;

/**
 * Created by pulasthi on 10/18/17.
 */
public class Driver {
    private static Options programOptions = new Options();
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
