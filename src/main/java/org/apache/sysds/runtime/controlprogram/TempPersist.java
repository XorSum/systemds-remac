package org.apache.sysds.runtime.controlprogram;

import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;

public class TempPersist {

    public static String variableName = "h" ;

    public static String rddName = "";

    public static ArrayList<JavaPairRDD> rdds = new ArrayList<>();

}
