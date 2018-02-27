package edu.iu.dsc.tws.mpiapps;

import edu.iu.dsc.tws.mpiapps.datacols.*;
import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.cli.*;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Program {
  private static Logger LOG = Logger.getLogger(Program.class.getName());
  // the collective to test
  int collective;

  // size of the datacols to test
  int dataSize;

  // number of iterations to use
  int iterations;

  private void readProgramArgs(String []args) {
    Options options = new Options();
    options.addOption("collective", true, "Type of collective, 0 Reduce, 1 All Reduce, 2 Broad cast");
    options.addOption("size", true, "Size of datacols to use");
    options.addOption("itr", true, "Number of iterations");

    CommandLineParser commandLineParser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = commandLineParser.parse(options, args);
      collective = Integer.parseInt(cmd.getOptionValue("collective"));
      dataSize = Integer.parseInt(cmd.getOptionValue("size"));
      iterations = Integer.parseInt(cmd.getOptionValue("itr"));
    } catch (ParseException e) {
      LOG.log(Level.SEVERE, "Failed to read the options", e);
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("program", options);
      throw new RuntimeException(e);
    }
  }

  public void execute(String []args) throws MPIException {
    readProgramArgs(args);
    int rank = MPI.COMM_WORLD.getRank();
    if (rank == 0) {
      System.out.println(String.format("Collective %d with size %d and iteration %d", collective, dataSize, iterations));
    }
    long startTime = System.currentTimeMillis();
    if (collective == 0) {
      Reduce r = new Reduce(dataSize, iterations);
      r.execute();
    } else if (collective == 1) {
      IntReduce iR = new IntReduce(dataSize, iterations);
      iR.execute();
    } else if (collective == 2) {
      Gather g = new Gather(dataSize, iterations);
      g.execute();
    } else if (collective == 3) {
      IntAllReduce allReduce = new IntAllReduce(dataSize, iterations);
      allReduce.execute();
    } else if (collective == 4) {
      AllReduce allReduce = new AllReduce(dataSize, iterations);
      allReduce.execute();
    }  else if (collective == 5) {
      AllGather allReduce = new AllGather(dataSize, iterations);
      allReduce.execute();
    } else if (collective == 6) {
      edu.iu.dsc.tws.mpiapps.cols.Reduce allReduce = new edu.iu.dsc.tws.mpiapps.cols.Reduce(dataSize, iterations);
      allReduce.execute();
    } else if (collective == 7) {
      edu.iu.dsc.tws.mpiapps.cols.AllReduce allReduce = new edu.iu.dsc.tws.mpiapps.cols.AllReduce(dataSize, iterations);
      allReduce.execute();
    } else if (collective == 8) {
      AllGather allGather = new AllGather(dataSize, iterations);
      allGather.execute();
    }

    long endTime = System.currentTimeMillis();
    long time = endTime - startTime;
    if (rank == 0) {
      System.out.println(String.format("Collective %d with size %d and iteration %d took %d", collective, dataSize, iterations, time));
    }
  }

  public static void main(String[] args) {
    try {
      MPI.Init(args);
      // execute the program
      Program p = new Program();
      p.execute(args);
      MPI.Finalize();
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Failed to tear down MPI");
    }
  }
}