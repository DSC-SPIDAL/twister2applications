package edu.iu.dsc.tws.spark.terasort;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

public class Driver {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<byte[], byte[]> input = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);
    JavaPairRDD<byte[], byte[]> sorted = input.repartitionAndSortWithinPartitions(new TeraSortPartitioner(), new ByteComparator());

    sorted.saveAsHadoopFile("out", byte[].class, byte[].class, ByteOutputFormat.class);
  }

  public static void dataFrameSort() {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();
    JavaSparkContext sc = new JavaSparkContext(conf);

    StructType schema = new StructType(new StructField[] {
        new StructField("key",
            DataTypes.createArrayType(DataTypes.ByteType), false,
            Metadata.empty()), new StructField("value",
        DataTypes.createArrayType(DataTypes.ByteType), false,
        Metadata.empty()) });
    JavaPairRDD<byte[], byte[]> input = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);

    JavaRDD<Row> rows = input.map(new Function<Tuple2<byte[], byte[]>, Row>(){
      private static final long serialVersionUID = -4332903997027358601L;

      @Override
      public Row call(Tuple2<byte[], byte[]> line) throws Exception {
        return RowFactory.create(line._1, line._2);
      }
    });
    Dataset<Row> wordDF = new SQLContext(sc).createDataFrame(rows, schema);
  }
}
