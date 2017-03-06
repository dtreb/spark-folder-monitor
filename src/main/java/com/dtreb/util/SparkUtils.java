package com.dtreb.util;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;

/**
 * Useful utility method used for interaction with Spark.
 *
 * @author dtreb
 */
public class SparkUtils {

    /**
     * Custom serialized tuple comparator.
     * Used to allow Spark serialize task.
     */
    public static class MyTupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return t1._2().compareTo(t2._2());
        }
    }
}
