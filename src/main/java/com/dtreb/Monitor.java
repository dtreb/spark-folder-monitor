package com.dtreb;

import com.dtreb.util.ParametersUtils;
import com.dtreb.util.SparkUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Application entry point.
 *
 * @author dtreb
 */
public class Monitor {

    public static void main(String[] args) throws InterruptedException {
        // Parse commandline options
        CommandLine commandLine = ParametersUtils.parseOptions(args);
        start(
                ParametersUtils.getFolder(commandLine),
                ParametersUtils.getResultsCount(commandLine),
                ParametersUtils.getIntervalInSeconds(commandLine)
        );
    }

    private static void start(final String folder, final int count, final int interval) throws InterruptedException {
        System.out.println("=== Parameters ===");
        System.out.println("Local folder path to monitor: " + Paths.get(folder).toAbsolutePath().toString());
        System.out.println("Check interval in seconds: " + interval);
        System.out.println("Results count: " + count);

        // Initialize local spark for available amount of processors/cores
        SparkConf config = new SparkConf()
                .setMaster("local[" + Runtime.getRuntime().availableProcessors() + "]")
                // we need to define at least 512Mb it or Spark will complain on start
                .set("spark.driver.memory", "1g")
                .setAppName("FolderMonitor");
        Duration duration = new Duration(interval * 1000);

        // Initialize Spark context
        JavaStreamingContext context = new JavaStreamingContext(config, duration);
        // Specify streaming folder (it will be checked on new files moved from other locations)
        JavaDStream<String> stream = context.textFileStream(folder);

        // Words count
        topWordsCount(stream, count);

        // Longest line
        longestLine(stream);

        // Start streaming
        context.start();

        System.out.println("\nStreaming is running...");
        context.awaitTermination();
    }

    /**
     * Prints words count for stream (text files in specified local folder).
     * @param stream {@link JavaDStream}
     */
    private static void topWordsCount(JavaDStream<String> stream, final int count) {
        // Break text into words
        JavaDStream<String> words = stream.flatMap(s -> Arrays.asList(ParametersUtils.SPACE.split(s)).iterator());

        // Map/reduce to get <word,count> pair
        JavaPairDStream<String, Integer> wordCounts = words
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        // Print pairs for limited amount of results
        wordCounts.foreachRDD((stringIntegerJavaPairRDD, time) -> {
            for (Tuple2<String, Integer> tuple : stringIntegerJavaPairRDD.top(count, new SparkUtils.MyTupleComparator())) {
                System.out.println("Word: \"" + tuple._1() + "\", Count: " + tuple._2());
            }
        });
    }

    /**
     * Prints longest line of text.
     * @param stream {@link JavaDStream}
     */
    private static void longestLine(JavaDStream<String> stream) {
        // Map lines by length
        JavaDStream<Integer> lengths = stream.map(line -> line.length());
        // Find longest one
        JavaDStream<Integer> length = lengths.reduce((a, b) -> (a > b) ? a : b);
        // Display length
        length.foreachRDD((rdd, time) -> {
            if (!rdd.isEmpty()) {
                System.out.println("The longest line: " + rdd.take(1) + " chars.");
            }
        });
    }
}
