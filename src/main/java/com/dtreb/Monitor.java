package com.dtreb;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Serializable;
import scala.Tuple2;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Pattern;

/**
 * Application entry point.
 *
 * @author dtreb
 */
public class Monitor {

    // Folder to monitor
    private static final String FOLDER = "monitor";
    // Pattern used to break text into words
    private static final Pattern SPACE = Pattern.compile(" ");
    // Amount of words to show counts for
    private static final int RESULTS_COUNT = 10;
    // Streaming folder check interval
    private static final int CHECK_INTERVAL_SECONDS = 10;

    public static void main(String[] args) throws InterruptedException {

        // Initialize local spark for available amount of processors/cores
        SparkConf config = new SparkConf()
                .setMaster("local[" + Runtime.getRuntime().availableProcessors() + "]")
                .setAppName("FolderMonitor");
        Duration duration = new Duration(CHECK_INTERVAL_SECONDS * 1000);

        // Initialize Spark context
        JavaStreamingContext context = new JavaStreamingContext(config, duration);
        // Specify streaming folder (it will be checked on new files moved from other locations)
        JavaDStream<String> stream = context.textFileStream(FOLDER);

        // Words count
        topWordsCount(stream, RESULTS_COUNT);

        // Start streaming
        context.start();

        System.out.println("Streaming for '" + Paths.get(FOLDER).toAbsolutePath().toString() + "' folder is running...");
        System.out.println("Check interval: " + CHECK_INTERVAL_SECONDS + " seconds");
        context.awaitTermination();
    }

    /**
     * Prints words count for stream (text files in specified local folder)
     * @param stream {@link JavaDStream}
     */
    private static void topWordsCount(JavaDStream<String> stream, final int count) {
        // Break text into words
        JavaDStream<String> words = stream.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        // Map/reduce to get <word,count> pair
        JavaPairDStream<String, Integer> wordCounts = words
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        // Print pairs for limited amount of results
        wordCounts.foreachRDD((stringIntegerJavaPairRDD, time) -> {
            for (Tuple2<String, Integer> tuple : stringIntegerJavaPairRDD.top(count, new MyTupleComparator())) {
                System.out.println("Word: \"" + tuple._1() + "\", Count: " + tuple._2());
            }
        });
    }

    /**
     * Custom serialized tuple comparator.
     * Used to allow Spark serialize task.
     */
    private static class MyTupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return t1._2().compareTo(t2._2());
        }
    }
}
