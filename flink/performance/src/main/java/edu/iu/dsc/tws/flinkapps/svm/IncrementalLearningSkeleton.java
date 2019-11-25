package edu.iu.dsc.tws.flinkapps.svm;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Skeleton for incremental machine learning algorithm consisting of a
 * pre-computed model, which gets updated for the new inputs and new input data
 * for which the job provides predictions.
 *
 * <p>This may serve as a base of a number of algorithms, e.g. updating an
 * incremental Alternating Least Squares model while also providing the
 * predictions.
 *
 * <p>This example shows how to use:
 * <ul>
 *   <li>Connected streams
 *   <li>CoFunctions
 *   <li>Tuple data types
 * </ul>
 */
public class IncrementalLearningSkeleton {


    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final int NUM_OF_DATAPOINTS = 10;
        final ParameterTool params = ParameterTool.fromArgs(args);
        final PegasosSGD pegasosSGD = new PegasosSGD();
        double[] w = new double[22];
        double[] finalModel = null;
        int ITERATIONS = 5;

        int windowLength = params.getInt("windowLength", 5);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Double[]> trainingData = env.addSource(new SVMTrainingDataSource());
        DataStream<Double[]> newData = env.addSource(new SVMTestingDataSource());

        // build new model on every second of new data
        long startTime = System.currentTimeMillis();


//        DataStream<Double[]> model = trainingData
//                .keyBy(0)
//                .countWindowAll(5)
//                .apply(new AllWindowFunction<Double[], Double[], GlobalWindow>() {
//
//                    protected Double[] buildPartialModel(Double [] d) {
//                        double[] all = ArrayUtils.toPrimitive(d);
//                        double[] x = Arrays.copyOfRange(all, 1, 23);
//                        pegasosSGD.onlineSGD(pegasosSGD.getNewW(), x, all[0]);
//                        return ArrayUtils.toObject(pegasosSGD.getNewW());
//                    }
//
//                    @Override
//                    public void apply(GlobalWindow globalWindow, Iterable<Double[]> values, Collector<Double[]> collector) throws Exception {
//                        Iterator<Double[]> d = values.iterator();
//                        for (int i = 0; i < ITERATIONS; i++) {
//                            while(d.hasNext()) {
//                                buildPartialModel(d.next());
//                            }
//                        }
//                        collector.collect(ArrayUtils.toObject(pegasosSGD.getNewW()));
//                    }
//                })
//                .countWindowAll(5)
//                .reduce(new ReduceFunction<Double[]>() {
//                    @Override
//                    public Double[] reduce(Double[] doubles, Double[] t1) throws Exception {
//                        Double[] newD = new Double[doubles.length];
//                        for (int i = 0; i < newD.length; i++) {
//                            newD[i] = (doubles[i] + t1[i]) / 2.0;
//                        }
//                        return newD;
//                    }
//                });


        DataStream<Double[]> model1 = trainingData
                .keyBy(0)
                .countWindow(windowLength)
                .apply(new WindowFunction<Double[], Double[], Tuple, GlobalWindow>() {

                    protected Double[] buildPartialModel(Double [] d) {
                        double[] all = ArrayUtils.toPrimitive(d);
                        double[] x = Arrays.copyOfRange(all, 1, 23);
                        pegasosSGD.onlineSGD(pegasosSGD.getNewW(), x, all[0]);
                        return ArrayUtils.toObject(pegasosSGD.getNewW());
                    }

                    @Override
                    public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<Double[]> values, Collector<Double[]> collector) throws Exception {
                        Iterator<Double[]> d = values.iterator();
                        for (int i = 0; i < ITERATIONS; i++) {
                            while(d.hasNext()) {
                                buildPartialModel(d.next());
                            }
                        }
                        collector.collect(ArrayUtils.toObject(pegasosSGD.getNewW()));
                    }
                })
                .keyBy(0)
                .countWindow(windowLength)
                .reduce(new ReduceFunction<Double[]>() {
                    @Override
                    public Double[] reduce(Double[] doubles, Double[] t1) throws Exception {
                        Double[] newD = new Double[doubles.length];
                        for (int i = 0; i < newD.length; i++) {
                            newD[i] = (doubles[i] + t1[i]) / 2.0;
                        }
                        return newD;
                    }
                });



        trainingData.print();
        //.assignTimestampsAndWatermarks(new LinearTimestamp())
        //.timeWindowAll(Time.of(1, TimeUnit.MILLISECONDS))
        //.apply(new PartialModelBuilder());

        // use partial model for newData
        DataStream<Double> prediction = newData.connect(model1).map(new CoMapFunction<Double[], Double[], Double>() {

            Double[] partialModel = null;

            Random random = new Random();

            @Override
            public Double map1(Double[] doubles) throws Exception {
                partialModel = new Double[doubles.length - 1];
                for (int i = 0; i < doubles.length - 1; i++) {
                    partialModel[i] = random.nextGaussian();
                }
                return predict(partialModel, doubles);
            }

            @Override
            public Double map2(Double[] doubles) throws Exception {
                partialModel = doubles;

                return 0.0;
            }

            public double predict(Double[] w1, Double[] x) {
                double[] xd = ArrayUtils.toPrimitive(x);
                double acc = 100;
                double[] w = ArrayUtils.toPrimitive(w1);
                double[] x1 = Arrays.copyOfRange(xd, 1, 23);
                double d = Matrix.dot(x1, w);
                double pred = Math.signum(d);
                if (xd[0] == pred) {
                    acc = 100.0;
                } else {
                    acc = 0;
                }
                //System.out.println(pred+"/"+ytest[i]+ " ==> " + d);
                return acc;
            }
        }).setParallelism(128);


        // emit result
        if (params.has("output")) {
            prediction.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            prediction.print();
        }

        // execute program
        env.execute("Streaming Incremental Learning");
        long endTime = System.currentTimeMillis();
        System.out.println("Training Time: " + (endTime - startTime) / 1000.0);
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Feeds new data for newData. By default it is implemented as constantly
     * emitting the Integer 1 in a loop.
     */
    public static class FiniteNewDataSource implements SourceFunction<Integer> {
        private static final long serialVersionUID = 1L;
        private static final int NUM_OF_DATAPOINTS = 10;
        private int counter;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            //Thread.sleep(1);
            while (counter < NUM_OF_DATAPOINTS) {
                ctx.collect(getNewData());
            }
        }

        @Override
        public void cancel() {
            // No cleanup needed
        }

        private Integer getNewData() throws InterruptedException {
            //Thread.sleep(1);
            counter++;
            return 1;
        }
    }

    public static class SVMTestingDataSource implements ParallelSourceFunction<Double[]> {
        private static final long serialVersionUID = 1L;
        private static final int NUM_OF_DATAPOINTS = 91701/128;

        private int counter = 0;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<Double[]> collector) throws Exception {
            while (counter < NUM_OF_DATAPOINTS) {
                collector.collect(getTrainingData());
            }
        }

        @Override
        public void cancel() {
            // No cleanup needed
        }

        private Double[] getTrainingData() throws InterruptedException {
            counter++;
            Double[] d = new Double[23];
            boolean status = random.nextBoolean();
            if (status) {
                d[0] = 1.0;
            } else {
                d[0] = -1.0;
            }

            for (int i = 1; i < 23; i++) {
                d[i] = random.nextGaussian();
            }
            return d;
        }
    }

    /**
     * Feeds new training data for the partial model builder. By default it is
     * implemented as constantly emitting the Integer 1 in a loop.
     */
    public static class FiniteTrainingDataSource implements SourceFunction<Integer> {
        private static final long serialVersionUID = 1L;
        private int counter = 0;

        @Override
        public void run(SourceContext<Integer> collector) throws Exception {
            while (counter < 100) {
                collector.collect(getTrainingData());
            }
        }

        @Override
        public void cancel() {
            // No cleanup needed
        }

        private Integer getTrainingData() throws InterruptedException {
            counter++;
            return 1;
        }
    }

    public static class SVMTrainingDataSource implements ParallelSourceFunction<Double[]> {
        private static final long serialVersionUID = 1L;
        private static final int NUM_OF_DATAPOINTS = 49990/128;

        private int counter = 0;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<Double[]> collector) throws Exception {
            while (counter < NUM_OF_DATAPOINTS) {
                collector.collect(getTrainingData());
            }
        }

        @Override
        public void cancel() {
            // No cleanup needed
        }

        private Double[] getTrainingData() throws InterruptedException {
            counter++;
            Double[] d = new Double[23];
            boolean status = random.nextBoolean();
            if (status) {
                d[0] = 1.0;
            } else {
                d[0] = -1.0;
            }

            for (int i = 1; i < 23; i++) {
                d[i] = random.nextGaussian();
            }
            return d;
        }
    }

    private static class LinearTimestamp implements AssignerWithPunctuatedWatermarks<Integer> {
        private static final long serialVersionUID = 1L;

        private long counter = 0L;

        @Override
        public long extractTimestamp(Integer element, long previousElementTimestamp) {
            return counter += 10L;
        }

        @Override
        public Watermark checkAndGetNextWatermark(Integer lastElement, long extractedTimestamp) {
            return new Watermark(counter - 1);
        }
    }

    /**
     * Builds up-to-date partial models on new training data.
     */
    public static class PartialModelBuilder implements AllWindowFunction<Integer, Double[], TimeWindow> {
        private static final long serialVersionUID = 1L;

        protected Double[] buildPartialModel(Iterable<Integer> values) {
            return new Double[]{1.};
        }

        @Override
        public void apply(TimeWindow window, Iterable<Integer> values, Collector<Double[]> out) throws Exception {
            out.collect(buildPartialModel(values));
        }
    }

    /**
     * Creates newData using the model produced in batch-processing and the
     * up-to-date partial model.
     * <p>
     * By default emits the Integer 0 for every newData and the Integer 1
     * for every model update.
     * </p>
     */
    public static class Predictor implements CoMapFunction<Integer, Double[], Integer> {
        private static final long serialVersionUID = 1L;

        Double[] batchModel = null;
        Double[] partialModel = null;

        @Override
        public Integer map1(Integer value) {
            // Return newData
            return predict(value);
        }

        @Override
        public Integer map2(Double[] value) {
            // Update model
            partialModel = value;
            batchModel = getBatchModel();
            return 1;
        }

        // pulls model built with batch-job on the old training data
        protected Double[] getBatchModel() {
            return new Double[]{0.};
        }

        // performs newData using the two models
        protected Integer predict(Integer inTuple) {
            return 0;
        }

    }

}
