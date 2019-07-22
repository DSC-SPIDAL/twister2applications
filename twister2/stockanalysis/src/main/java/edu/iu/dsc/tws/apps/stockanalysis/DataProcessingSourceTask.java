package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.executor.ExecutorContext;
import edu.iu.dsc.tws.api.task.nodes.BaseSource;
import edu.iu.dsc.tws.apps.stockanalysis.utils.CleanMetric;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Record;
import edu.iu.dsc.tws.apps.stockanalysis.utils.Utils;
import edu.iu.dsc.tws.apps.stockanalysis.utils.VectorPoint;
import edu.iu.dsc.tws.data.api.formatters.LocalCompleteTextInputPartitioner;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.logging.Logger;

public class DataProcessingSourceTask extends BaseSource {
    private static final Logger LOG = Logger.getLogger(DataPreprocessingSourceTask.class.getName());

    private static final long serialVersionUID = -5190777711234234L;

    private Map<Integer, VectorPoint> currentPoints = new HashMap<>();
    private Map<String, CleanMetric> metrics = new HashMap<>();
    private TreeMap<String, List<Date>> dates = new TreeMap<>();

    private int vectorCounter = 0;

    private String inputFile;
    private String outputFile;
    private String startDate;
    private BufferedReader bufRead;
    private FileReader input;
    private Record record;

    private DataSource<?, ?> source;
    private InputSplit<?> inputSplit;

    public DataProcessingSourceTask(String inputfile, String outputfile, String startdate) {
        this.inputFile = inputfile;
        this.outputFile = outputfile;
        this.startDate = startdate;
    }

    @Override
    public void execute() {
        /*inputSplit = source.getNextSplit(context.taskIndex());
        while (inputSplit != null) {
            try {
                while (!inputSplit.reachedEnd()) {
                    String value = String.valueOf(inputSplit.nextRecord(null));
                    if ((record = getRecord(value, null, false)) != null) {
                        LOG.info("Received Record Value:" + record + "\t" + record.getDate());
                        context.write(Context.TWISTER2_DIRECT_EDGE, record);
                    }
                }
                inputSplit = source.getNextSplit(context.taskIndex());
            } catch (IOException e) {
                LOG.log(Level.SEVERE, "Failed to read the input", e);
            }
        }*/

//        String value;
//
//        inputSplit = source.getNextSplit(context.taskIndex());
//        if (inputSplit != null) {
//            try {
//                if (inputSplit.nextRecord(null) != null) {
//                    value = String.valueOf(inputSplit.nextRecord(null));
//                    writeRecord(value);
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        inputSplit = source.getNextSplit(context.taskIndex());

        readFile();
        //initiateProcess();
    }

    private void readFile() {
        try {
            Record record;
            if ((record = Utils.parseFile(bufRead, null, false)) != null) {
                LOG.info("Record to write:" + record);
                context.write(Context.TWISTER2_DIRECT_EDGE, record);
            }
        } catch (IOException ioe) {
            throw new RuntimeException("IO Exception Occured" + ioe.getMessage());
        }
    }

    public void prepare(Config cfg, TaskContext context) {
        super.prepare(cfg, context);
        ExecutionRuntime runtime = (ExecutionRuntime) cfg.get(ExecutorContext.TWISTER2_RUNTIME_OBJECT);
        this.source = runtime.createInput(cfg, context, new LocalCompleteTextInputPartitioner(
                new Path(inputFile), context.getParallelism(), config));

        try {
            input = new FileReader(inputFile);
            bufRead = new BufferedReader(input);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("IOException Occured:" + e.getMessage());
        }
    }

    private Record getRecord(String value, CleanMetric metric, boolean convert) {
        try {
            if (value != null) {
                String[] array = value.trim().split(",");
                if (array.length >= 3) {
                    int permNo = Integer.parseInt(array[0]);
                    Date date = Utils.formatter.parse(array[1]);
                    if (date == null) {
                        LOG.info("Date null...............................");
                    }
                    String stringSymbol = array[2];
                    if (array.length >= 7) {
                        double price = -1;
                        if (!array[5].equals("")) {
                            price = Double.parseDouble(array[5]);
                            if (convert) {
                                if (price < 0) {
                                    price *= -1;
                                    if (metric != null) {
                                        metric.negativeCount++;
                                    }
                                }
                            }
                        }

                        double factorToAdjPrice = 0;
                        if (!"".equals(array[4].trim())) {
                            factorToAdjPrice = Double.parseDouble(array[4]);
                        }

                        double factorToAdjVolume = 0;
                        if (!"".equals(array[3].trim())) {
                            factorToAdjVolume = Double.parseDouble(array[3]);
                        }

                        int volume = 0;
                        if (!array[6].equals("")) {
                            volume = Integer.parseInt(array[6]);
                        }

                        return new Record(price, permNo, date, array[1], stringSymbol, volume, factorToAdjPrice, factorToAdjVolume);
                    } else {
                        return new Record(-1, permNo, date, array[1], stringSymbol, 0, 0, 0);
                    }
                }
            }
        } catch (ParseException e) {
            throw new RuntimeException("Failed to read content from file", e);
        }

        return null;
    }
}
