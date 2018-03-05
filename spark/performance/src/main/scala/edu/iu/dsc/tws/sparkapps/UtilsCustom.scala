package edu.iu.dsc.tws.sparkapps

import com.google.common.base.Optional
import org.apache.commons.cli._

/**
  * Created by pulasthi on 7/3/16.
  */
object UtilsCustom {

  val PROGRAM_NAME: String = "K-Means"
  val ERR_PROGRAM_ARGUMENTS_PARSING_FAILED: String = "Argument parsing failed!"
  val ERR_INVALID_PROGRAM_ARGUMENTS: String = "Invalid program arguments!"
  val ERR_EMPTY_FILE_NAME: String = "File name is null or empty!"

  def parseCommandLineArguments(args: Array[String], opts: Options): Optional[CommandLine] = {
    val optParser: CommandLineParser = new GnuParser();
    try {
      return Optional.fromNullable(optParser.parse(opts, args))
    }
    catch {
      case e: ParseException => {
        e.printStackTrace
      }
    }
    return Optional.fromNullable(null);
  }
}
