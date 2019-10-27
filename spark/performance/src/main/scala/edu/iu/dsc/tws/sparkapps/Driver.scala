package edu.iu.dsc.tws.sparkapps

import com.google.common.base.Optional
import edu.iu.dsc.tws.sparkapps.batch.{Reduce, Gather}
import org.apache.commons.cli.{HelpFormatter, CommandLine, Options}

/**
  * Created by pulasthi on 3/5/18.
  */
object Driver {
  var programOptions: Options = new Options();
  def main(args: Array[String]): Unit = {
    programOptions.addOption("size", true, "Size of data")
    programOptions.addOption("iter", true, "Num iter")
    programOptions.addOption("col", true, "Colmun")
    programOptions.addOption("para", true, "Parallelism")

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
    if (col == 0) {
      var reduce = new Reduce(para, size, iter)
      reduce.execute();
    }else if (col == 1) {
      var gather = new Gather(para, size, iter)
      gather.execute();
    }
    else if (col == 2) {
      var reduce = new streaming.Reduce(para, size, iter)
      reduce.execute();
    }else if (col == 3) {
      var gather = new streaming.Gather(para, size, iter)
      gather.execute();
    }
  }
}
