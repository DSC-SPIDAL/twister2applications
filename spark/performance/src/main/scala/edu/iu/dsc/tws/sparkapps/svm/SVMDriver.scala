package edu.iu.dsc.tws.sparkapps.svm

import com.google.common.base.Optional
import edu.iu.dsc.tws.sparkapps.batch.{Gather, Reduce}
import edu.iu.dsc.tws.sparkapps.{UtilsCustom, streaming}
import org.apache.commons.cli.{CommandLine, HelpFormatter, Options}

object SVMDriver {
  var programOptions: Options = new Options();
  def main(args: Array[String]): Unit = {
    programOptions.addOption("size", true, "Size of data")
    programOptions.addOption("iter", true, "Num iter")
    programOptions.addOption("col", true, "Colmun")
    programOptions.addOption("para", true, "Parallelism")
    programOptions.addOption("windowLength", true, "Window Length")
    programOptions.addOption("slidingLength", true, "Sliding Length")
    programOptions.addOption("windowType", true, "WindowType (tumbling or sliding)")
    programOptions.addOption("features", true, "features")

    val parserResult: Optional[CommandLine] = UtilsCustom.parseCommandLineArguments(args, programOptions)
    if (!parserResult.isPresent) {
      println(UtilsCustom.ERR_PROGRAM_ARGUMENTS_PARSING_FAILED)
      new HelpFormatter().printHelp(UtilsCustom.PROGRAM_NAME, programOptions)
      return
    }

    val cmd: CommandLine = parserResult.get

    val size: Int = cmd.getOptionValue("size").toInt
    val iter: Int = cmd.getOptionValue("iter").toInt
    val para: Int = cmd.getOptionValue("para").toInt

    val windowLength: Int = cmd.getOptionValue("windowLength").toInt
    val slidingLength: Int = cmd.getOptionValue("slidingLength").toInt
    val windowType: String = cmd.getOptionValue("windowType")
    val features: Int = cmd.getOptionValue("features").toInt

    val isTumbling = if (windowType.equalsIgnoreCase("TUMBLING"))  true else false

    var streamingSVM = new StreamingSVM(para, features, iter, windowLength, slidingLength, isTumbling)
    streamingSVM.execute()
  }
}
