package com.dtreb;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;


public class Monitor {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static final int RESULTS_COUNT = 10;
    private static final int CHECK_INTERVAL_SECONDS = 10;

    public static void main(String[] args) throws InterruptedException {
        SparkConf config = new SparkConf()
                .setMaster("local[" + Runtime.getRuntime().availableProcessors() + "]")
                .setAppName("FolderMonitor");
        Duration duration = new Duration(CHECK_INTERVAL_SECONDS * 1000);
        JavaStreamingContext context = new JavaStreamingContext(config, duration);
        JavaDStream<String> stream = context.textFileStream("monitor");

        // Words count
        wordsCount(stream);

        context.start();
        context.awaitTermination();
    }

    public static void wordsCount(JavaDStream<String> stream) {
        JavaDStream<String> words = stream.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
                JavaPairDStream < String, Integer > wordCounts = words
                        .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                        .reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.foreachRDD((VoidFunction2<JavaPairRDD<String, Integer>, Time>) (stringIntegerJavaPairRDD, time) -> {
            for (Tuple2<String, Integer> tuple : stringIntegerJavaPairRDD.take(RESULTS_COUNT)) {
                System.out.println("Word: " + tuple._1() + ", Count: " + tuple._2());
            }
        });
    }
}
