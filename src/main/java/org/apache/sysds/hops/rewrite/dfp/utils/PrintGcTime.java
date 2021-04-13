package org.apache.sysds.hops.rewrite.dfp.utils;

import static org.apache.sysds.utils.Statistics.getJVMgcCount;
import static org.apache.sysds.utils.Statistics.getJVMgcTime;

public class PrintGcTime {

    public static void printGcTime() {

        System.out.println("Total JVM GC count:\t\t" + getJVMgcCount() + ".\n");
        System.out.println("Total JVM GC time:\t\t" + ((double) getJVMgcTime()) / 1000 + " sec.\n");
    }

}
