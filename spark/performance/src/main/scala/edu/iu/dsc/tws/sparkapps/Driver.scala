package edu.iu.dsc.tws.sparkapps

import com.google.common.base.Optional
import edu.iu.dsc.tws.sparkapps.batch.{Gather, Reduce}
import edu.iu.dsc.tws.sparkapps.test.GeneralTest
import org.apache.commons.cli.{CommandLine, HelpFormatter, Options}

/**
 * Created by pulasthi on 3/5/18.
 */
object Driver {
  var programOptions: Options = new Options();

  def main(args: Array[String]): Unit = {
    programOptions.addOption("size", true, "Size of data")
    programOptions.addOption("iter", true, "Num iter")
    programOptions.addOption("batchSize", true, "batchSize")
    programOptions.addOption("qDelay", true, "Queue delay for adding elements in stream processing")
    programOptions.addOption("col", true, "Colmun")
    programOptions.addOption("para", true, "Parallelism")
    programOptions.addOption("logPath", true, "Log Path")

    val parserResult: Optional[CommandLine] = UtilsCustom.parseCommandLineArguments(args, programOptions)
    if (!parserResult.isPresent) {
      println(UtilsCustom.ERR_PROGRAM_ARGUMENTS_PARSING_FAILED)
      new HelpFormatter().printHelp(UtilsCustom.PROGRAM_NAME, programOptions)
      return
    }

    val cmd: CommandLine = parserResult.get

    val size: Int = cmd.getOptionValue("size").toInt
    val iter: Int = cmd.getOptionValue("iter").toInt
    val col: Int = cmd.getOptionValue("col").toInt
    val para: Int = cmd.getOptionValue("para").toInt
    val batchSize: Int = cmd.getOptionValue("batchSize").toInt
    val qDelay: Int = cmd.getOptionValue("qDelay").toInt
    val logPath: String = cmd.getOptionValue("logPath").toString

    if (col == 0) {
      var reduce = new Reduce(para, size, iter)
      reduce.execute();
    } else if (col == 1) {
      var gather = new Gather(para, size, iter)
      gather.execute();
    }
    else if (col == 2) {
      var reduce = new streaming.Reduce(para, size, iter, batchSize, qDelay, logPath)
      reduce.execute();
    }
    else if (col == 3) {
      var gather = new streaming.Gather(para, size, iter)
      gather.execute();
    }
    else if (col == 4) {
      var test = new GeneralTest(para, size, iter, batchSize, qDelay, logPath)
      test.execute();
    }
  }
}
