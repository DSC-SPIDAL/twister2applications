package edu.iu.dsc.tws.spark.terasort;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Driver {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<byte[], byte[]> input = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);
    JavaPairRDD<byte[], byte[]> sorted = input.repartitionAndSortWithinPartitions(new TeraSortPartitioner(input.partitions().size()), new ByteComparator());

    sorted.saveAsHadoopFile("out", byte[].class, byte[].class, ByteOutputFormat.class);
    sc.stop();
  }

//  public static void dataFrameSort() {
//    SparkConf conf = new SparkConf().setAppName("terasort");
//    Configuration configuration = new Configuration();
//    JavaSparkContext sc = new JavaSparkContext(conf);
//
//    StructType schema = new StructType(new StructField[] {
//        new StructField("key",
//            DataTypes.createArrayType(DataTypes.ByteType), false,
//            Metadata.empty()), new StructField("value",
//        DataTypes.createArrayType(DataTypes.ByteType), false,
//        Metadata.empty()) });
//    JavaPairRDD<byte[], byte[]> input = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);
//
//    JavaRDD<Row> rows = input.map(new Function<Tuple2<byte[], byte[]>, Row>(){
//      private static final long serialVersionUID = -4332903997027358601L;
//
//      @Override
//      public Row call(Tuple2<byte[], byte[]> line) throws Exception {
//        return RowFactory.create(line._1, line._2);
//      }
//    });
//    Dataset<Row> wordDF = new SQLContext(sc).createDataFrame(rows, schema);
//  }
}
