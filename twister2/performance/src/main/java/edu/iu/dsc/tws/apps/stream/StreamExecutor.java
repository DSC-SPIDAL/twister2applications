package edu.iu.dsc.tws.apps.stream;

import edu.iu.dsc.tws.apps.storm.Executor;
import edu.iu.dsc.tws.apps.utils.JobParameters;

import java.util.logging.Level;
import java.util.logging.Logger;

public class StreamExecutor implements Runnable {
  private static final Logger LOG = Logger.getLogger(Executor.class.getName());

  private ExternalSource source;

  private JobParameters jobParameters;

  private int executorId;

  public StreamExecutor(int executorId, ExternalSource source, JobParameters jobParameters) {
    this.source = source;
    this.executorId = executorId;
    this.jobParameters = jobParameters;
  }

  public void run() {
    try {
      while (true) {
        if (!source.isStop()) {
          while (source.execute());
        }
        source.progress();
      }
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "Error occured", t);
    }
  }
}
