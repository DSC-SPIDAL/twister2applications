package edu.iu.dsc.tws.flinkapps.svm;

import edu.iu.dsc.tws.flinkapps.data.CollectiveDoubleData;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public class SVMReduce {


    double[] w = new double[22];
    double[] finalModel = null;
    int ITERATIONS = 5;

    int size;
    int iterations;
    StreamExecutionEnvironment env;
    String outFile;

    public SVMReduce(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public SVMReduce(int size, int iterations, StreamExecutionEnvironment env, String outFile) {
        this.size = size;
        this.iterations = iterations;
        this.env = env;
        this.outFile = outFile;
    }

    public void execute() {
        DataStream<CollectiveDoubleData> stringStream = env.addSource(new RichParallelSourceFunction<CollectiveDoubleData>() {
            int i = 1;
            int count = 0;
            int size = 0;
            int iterations = 49990/128;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void run(SourceContext<CollectiveDoubleData> sourceContext) throws Exception {
                while (count < iterations) {
                    CollectiveDoubleData i = new CollectiveDoubleData(23);
                    sourceContext.collect(i);
                    count++;
                }
            }

            @Override
            public void cancel() {
            }
        }).setParallelism(128);

        stringStream.map(new RichMapFunction<CollectiveDoubleData, Tuple2<Integer, CollectiveDoubleData>>() {
            @Override
            public Tuple2<Integer, CollectiveDoubleData> map(CollectiveDoubleData s) throws Exception {
                return new Tuple2<Integer, CollectiveDoubleData>(0, s);
            }
        })
                .keyBy(0)
                .countWindow(5)
                .apply(new WindowFunctionClass())
                .countWindowAll(5)
                .reduce(new ReduceFunction<CollectiveDoubleData>() {
                    @Override
                    public CollectiveDoubleData reduce(CollectiveDoubleData o, CollectiveDoubleData t1) throws Exception {
                        return add(o, t1);
                    }
                }).addSink(new RichSinkFunction<CollectiveDoubleData>() {

            long start;
            int count = 0;
            int iterations=10000;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void invoke(CollectiveDoubleData value, Context context) throws Exception {
                if (count == 0) {
                    start = System.nanoTime();
                }
                count++;
                if (count >= iterations) {
                    System.out.println("Final: " + count + " " + (System.nanoTime() - start) / 1000000 + " ");
                }
            }
        });

        //stringStream.print();

        DataStream<CollectiveDoubleData> stringStream2 = env.addSource(new RichParallelSourceFunction<CollectiveDoubleData>() {
            int i = 1;
            int count = 0;
            int size = 0;
            int iterations = 91701/128;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void run(SourceContext<CollectiveDoubleData> sourceContext) throws Exception {
                while (count < iterations) {
                    CollectiveDoubleData i = new CollectiveDoubleData(23);
                    sourceContext.collect(i);
                    count++;
                }
            }

            @Override
            public void cancel() {
            }
        }).setParallelism(128);

        DataStream<Double> prediction = stringStream2.connect(stringStream).map(new CoMapFunction<CollectiveDoubleData, CollectiveDoubleData, Double>() {
            CollectiveDoubleData partialModel = null;
            Random random = new Random();
            @Override
            public Double map1(CollectiveDoubleData collectiveDoubleData) throws Exception {
               double[] d1 = new double[collectiveDoubleData.getList().length - 1];
                for (int i = 0; i < collectiveDoubleData.getList().length - 1; i++) {
                    d1[i] = random.nextGaussian();
                }
                partialModel = new CollectiveDoubleData(d1);
                return predict(partialModel.getList(), collectiveDoubleData.getList());
            }

            @Override
            public Double map2(CollectiveDoubleData collectiveDoubleData) throws Exception {
                partialModel = collectiveDoubleData;
                return 0.0;
            }

            public double predict(double[] w, double[] x) {

                double acc = 100;
                double[] x1 = Arrays.copyOfRange(x, 1, 23);
                double d = Matrix.dot(x1, w);
                double pred = Math.signum(d);
                if (x[0] == pred) {
                    acc = 100.0;
                } else {
                    acc = 0;
                }
                //System.out.println(pred+"/"+ytest[i]+ " ==> " + d);
                return acc;
            }
        }).setParallelism(128);

        stringStream.print();
        stringStream2.print();
        prediction.print();




//            .addSink(new RichSinkFunction<Tuple2<Integer, CollectiveDoubleData>>() {
//                long start;
//                int count = 0;
//                int iterations;
//
//                @Override
//                public void open(Configuration parameters) throws Exception {
//                    super.open(parameters);
//                    ParameterTool p = (ParameterTool)
//                            getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
//                    iterations = p.getInt("itr", 10000);
//                    System.out.println("7777 iterations: " + iterations);
//                }
//
//                @Override
//                public void invoke(Tuple2<Integer, CollectiveDoubleData> integerStringTuple2) throws Exception {
//                    if (count == 0) {
//                        start = System.nanoTime();
//                    }
//                    count++;
//                    if (count >= iterations) {
//                        System.out.println("Final: " + count + " " + (System.nanoTime() - start) / 1000000 + " " + (integerStringTuple2.f1));
//                    }
//                }
//            });

    }

    private static CollectiveDoubleData add(CollectiveDoubleData i, CollectiveDoubleData j) {
        double[] r = new double[i.getList().length];
        for (int k = 0; k < i.getList().length; k++) {
            r[k] = ((i.getList()[k] + j.getList()[k]));
        }
        return new CollectiveDoubleData(r);
    }

    private class WindowFunctionClass implements WindowFunction<Tuple2<Integer, CollectiveDoubleData>, CollectiveDoubleData, Tuple, GlobalWindow>, Serializable {

        PegasosSGD pegasosSGD = null;

        public WindowFunctionClass() {
            this.pegasosSGD = new PegasosSGD();
        }

        protected Double[] buildPartialModel(double[] all) {
            double[] x = Arrays.copyOfRange(all, 1, 23);
            this.pegasosSGD.onlineSGD(this.pegasosSGD.getNewW(), x, all[0]);
            return ArrayUtils.toObject(this.pegasosSGD.getNewW());
        }

        @Override
        public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<Tuple2<Integer, CollectiveDoubleData>> values, Collector<CollectiveDoubleData> collector) throws Exception {

            Iterator<Tuple2<Integer, CollectiveDoubleData>> d = values.iterator();
            for (int i = 0; i < 5; i++) {
                while (d.hasNext()) {
                    CollectiveDoubleData collectiveDoubleData = d.next().f1;
                    buildPartialModel(collectiveDoubleData.getList());
                }
            }
            double[] newW = pegasosSGD.getNewW();
            collector.collect(new CollectiveDoubleData(newW));
        }


    }
}
